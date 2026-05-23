use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:0")]
    listen: SocketAddr,

    #[arg(long)]
    session_file: PathBuf,

    #[arg(long)]
    log_dir: PathBuf,

    #[arg(long, default_value = "wormhole")]
    profile: String,

    #[arg(long, default_value_t = false)]
    full_auto: bool,

    #[arg(long)]
    token: String,

    #[arg(long)]
    codex_bin: Option<PathBuf>,
}

#[derive(Clone)]
struct AppState {
    token: String,
    log_dir: PathBuf,
    profile: String,
    full_auto: bool,
    codex_bin: PathBuf,
    sessions: Arc<Mutex<HashMap<String, SessionHandle>>>,
}

struct SessionHandle {
    session: AgentSession,
    child: Child,
}

#[derive(Debug, Deserialize)]
struct StartSessionRequest {
    prompt: String,
    cwd: Option<String>,
    profile: Option<String>,
    full_auto: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
struct AgentSession {
    id: String,
    status: String,
    prompt: Option<String>,
    cwd: Option<String>,
    started_at: u64,
    ended_at: Option<u64>,
    log_path: Option<String>,
}

#[derive(Debug, Serialize)]
struct SessionFile<'a> {
    endpoint: &'a str,
    token: &'a str,
}

#[derive(Debug, Serialize)]
struct AuditEvent<'a, T: Serialize> {
    timestamp: u64,
    session_id: &'a str,
    event: &'a str,
    #[serde(flatten)]
    payload: T,
}

#[derive(Debug, Serialize)]
struct SessionStartedPayload<'a> {
    status: &'a str,
    prompt: &'a str,
    cwd: Option<&'a str>,
    codex_bin: &'a str,
}

#[derive(Debug, Serialize)]
struct StreamPayload<'a> {
    stream: &'a str,
    line: &'a str,
}

#[derive(Debug, Serialize)]
struct SessionEndedPayload {
    status: String,
    exit_code: Option<i32>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    fs::create_dir_all(&cli.log_dir)
        .await
        .with_context(|| format!("failed to create log dir {}", cli.log_dir.display()))?;
    if let Some(parent) = cli.session_file.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create session dir {}", parent.display()))?;
    }

    let codex_bin = match cli.codex_bin {
        Some(path) => path,
        None => find_codex_binary().context("failed to locate codex binary")?,
    };

    let listener = TcpListener::bind(cli.listen).await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    write_session_file(&cli.session_file, &endpoint, &cli.token).await?;

    let state = AppState {
        token: cli.token,
        log_dir: cli.log_dir,
        profile: cli.profile,
        full_auto: cli.full_auto,
        codex_bin,
        sessions: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/sessions", post(start_session).get(list_sessions))
        .route("/sessions/{session_id}", get(get_session))
        .route("/sessions/{session_id}/events", get(read_session_events))
        .route("/sessions/{session_id}/stop", post(stop_session))
        .with_state(state);

    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(state): State<AppState>, headers: HeaderMap) -> Response {
    match authorize(&state, &headers) {
        Ok(()) => Json(serde_json::json!({ "ok": true })).into_response(),
        Err(response) => response,
    }
}

async fn start_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<StartSessionRequest>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }

    let prompt = req.prompt.trim().to_string();
    if prompt.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "prompt is required");
    }

    match spawn_codex_session(state.clone(), prompt, req.cwd, req.profile, req.full_auto).await {
        Ok(session) => Json(session).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string()),
    }
}

async fn list_sessions(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    reap_finished_children(&state).await;
    let sessions = state
        .sessions
        .lock()
        .await
        .values()
        .map(|handle| handle.session.clone())
        .collect::<Vec<_>>();
    Json(sessions).into_response()
}

async fn get_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(session_id): AxumPath<String>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    reap_finished_children(&state).await;
    let sessions = state.sessions.lock().await;
    match sessions.get(&session_id) {
        Some(handle) => Json(handle.session.clone()).into_response(),
        None => error_response(StatusCode::NOT_FOUND, "session not found"),
    }
}

async fn stop_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(session_id): AxumPath<String>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    let mut sessions = state.sessions.lock().await;
    let Some(handle) = sessions.get_mut(&session_id) else {
        return error_response(StatusCode::NOT_FOUND, "session not found");
    };
    let _ = handle.child.kill().await;
    handle.session.status = "stopping".to_string();
    Json(handle.session.clone()).into_response()
}

async fn read_session_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(session_id): AxumPath<String>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    if !is_safe_session_id(&session_id) {
        return error_response(StatusCode::BAD_REQUEST, "invalid session id");
    }
    let path = state.log_dir.join(format!("{session_id}.jsonl"));
    match fs::read_to_string(path).await {
        Ok(log) => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/x-ndjson")
            .body(Body::from(log))
            .unwrap_or_else(|_| error_response(StatusCode::INTERNAL_SERVER_ERROR, "response error")),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            error_response(StatusCode::NOT_FOUND, "log not found")
        }
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string()),
    }
}

async fn spawn_codex_session(
    state: AppState,
    prompt: String,
    cwd: Option<String>,
    profile: Option<String>,
    full_auto: Option<bool>,
) -> Result<AgentSession> {
    let session_id = Uuid::now_v7().to_string();
    let log_path = state.log_dir.join(format!("{session_id}.jsonl"));
    let profile = profile.unwrap_or_else(|| state.profile.clone());
    let full_auto = full_auto.unwrap_or(state.full_auto);

    let mut cmd = Command::new(&state.codex_bin);
    cmd.arg("exec")
        .arg("--json")
        .arg("--skip-git-repo-check")
        .arg("--profile")
        .arg(profile);
    if full_auto {
        cmd.arg("--dangerously-bypass-approvals-and-sandbox");
    }
    if let Some(cwd) = cwd.as_deref().filter(|value| !value.trim().is_empty()) {
        cmd.arg("--cd").arg(cwd);
    }
    cmd.arg(&prompt)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to spawn {}", state.codex_bin.display()))?;

    write_audit(
        &log_path,
        &AuditEvent {
            timestamp: unix_secs(),
            session_id: &session_id,
            event: "session_started",
            payload: SessionStartedPayload {
                status: "running",
                prompt: &prompt,
                cwd: cwd.as_deref(),
                codex_bin: &state.codex_bin.to_string_lossy(),
            },
        },
    )
    .await?;

    if let Some(stdout) = child.stdout.take() {
        tokio::spawn(copy_lines_to_audit(
            log_path.clone(),
            session_id.clone(),
            "stdout",
            stdout,
        ));
    }
    if let Some(stderr) = child.stderr.take() {
        tokio::spawn(copy_lines_to_audit(
            log_path.clone(),
            session_id.clone(),
            "stderr",
            stderr,
        ));
    }

    let session = AgentSession {
        id: session_id.clone(),
        status: "running".to_string(),
        prompt: Some(prompt),
        cwd,
        started_at: unix_secs(),
        ended_at: None,
        log_path: Some(log_path.display().to_string()),
    };

    state.sessions.lock().await.insert(
        session_id,
        SessionHandle {
            session: session.clone(),
            child,
        },
    );
    Ok(session)
}

async fn reap_finished_children(state: &AppState) {
    let mut ended = Vec::new();
    {
        let mut sessions = state.sessions.lock().await;
        for (id, handle) in sessions.iter_mut() {
            if handle.session.ended_at.is_some() {
                continue;
            }
            match handle.child.try_wait() {
                Ok(Some(status)) => {
                    handle.session.ended_at = Some(unix_secs());
                    handle.session.status = if status.success() {
                        "completed".to_string()
                    } else {
                        "failed".to_string()
                    };
                    ended.push((id.clone(), handle.session.clone(), status.code()));
                }
                Ok(None) => {}
                Err(_) => {
                    handle.session.ended_at = Some(unix_secs());
                    handle.session.status = "unknown".to_string();
                    ended.push((id.clone(), handle.session.clone(), None));
                }
            }
        }
    }
    for (id, session, exit_code) in ended {
        if let Some(log_path) = session.log_path.as_deref() {
            let _ = write_audit(
                Path::new(log_path),
                &AuditEvent {
                    timestamp: unix_secs(),
                    session_id: &id,
                    event: "session_ended",
                    payload: SessionEndedPayload {
                        status: session.status,
                        exit_code,
                    },
                },
            )
            .await;
        }
    }
}

async fn copy_lines_to_audit<R>(log_path: PathBuf, session_id: String, stream: &'static str, reader: R)
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                let _ = write_audit(
                    &log_path,
                    &AuditEvent {
                        timestamp: unix_secs(),
                        session_id: &session_id,
                        event: "codex_output",
                        payload: StreamPayload {
                            stream,
                            line: &line,
                        },
                    },
                )
                .await;
            }
            Ok(None) => break,
            Err(err) => {
                let message = err.to_string();
                let _ = write_audit(
                    &log_path,
                    &AuditEvent {
                        timestamp: unix_secs(),
                        session_id: &session_id,
                        event: "stream_error",
                        payload: StreamPayload {
                            stream,
                            line: &message,
                        },
                    },
                )
                .await;
                break;
            }
        }
    }
}

async fn write_audit<T: Serialize>(path: &Path, event: &T) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    let mut line = serde_json::to_vec(event)?;
    line.push(b'\n');
    file.write_all(&line).await?;
    Ok(())
}

async fn write_session_file(path: &Path, endpoint: &str, token: &str) -> Result<()> {
    let raw = serde_json::to_vec_pretty(&SessionFile { endpoint, token })?;
    fs::write(path, raw)
        .await
        .with_context(|| format!("failed to write {}", path.display()))
}

fn authorize(state: &AppState, headers: &HeaderMap) -> Result<(), Response> {
    let Some(value) = headers.get("authorization").and_then(|v| v.to_str().ok()) else {
        return Err(error_response(StatusCode::UNAUTHORIZED, "missing bearer token"));
    };
    let expected = format!("Bearer {}", state.token);
    if value != expected {
        return Err(error_response(StatusCode::FORBIDDEN, "invalid bearer token"));
    }
    Ok(())
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

fn find_codex_binary() -> Result<PathBuf> {
    if let Ok(path) = std::env::var("WORMHOLE_CODEX") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
    }
    if let Ok(current) = std::env::current_exe()
        && let Some(dir) = current.parent()
    {
        for name in ["codex.exe", "codex"] {
            let candidate = dir.join(name);
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
    }
    find_in_path("codex.exe")
        .or_else(|| find_in_path("codex"))
        .ok_or_else(|| anyhow!("codex binary not found"))
}

fn find_in_path(name: &str) -> Option<PathBuf> {
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|dir| dir.join(name))
        .find(|path| path.is_file())
}

fn is_safe_session_id(session_id: &str) -> bool {
    !session_id.is_empty()
        && session_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::is_safe_session_id;

    #[test]
    fn safe_session_id_rejects_paths() {
        assert!(is_safe_session_id("019ab778-a7fd-7121-9def-0cc725522e55"));
        assert!(!is_safe_session_id("../secret"));
        assert!(!is_safe_session_id("a.jsonl"));
        assert!(!is_safe_session_id(""));
    }
}
