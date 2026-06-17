#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use clap::Parser;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use uuid::Uuid;

use agent_core::{
    codex_exec_argv, resolve_codex_exec_sandbox, ComAutomationError, ComAutomationPlan,
    ComRecipeCatalog, CodexExecPolicyInput, CodexExecSandbox, CodexExecTurn, GuiAutomationError,
    GuiAutomationPlan, GuiControlMode, GuiEngine, RecipeCatalog, RemoteAgentCapabilities,
    RemoteAgentTaskRequest, RemoteAgentTaskResult, TaskArtifact, TaskArtifactKind,
    TaskFailureReason, TaskKind, TaskStatus, batch_request_from_remote_task,
    bundled_com_recipes_dir, bundled_recipes_dir, classify_com_failure, classify_gui_failure,
    com_automation_supported, expand_com_automation, expand_gui_automation,
    remote_task_result_from_batch, summarize_stderr, tail_task_audit_ndjson,
};
use compute_core::BatchAdapterRegistry;

mod service_control;

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

    #[arg(long)]
    recipes_dir: Option<PathBuf>,

    #[arg(long)]
    com_recipes_dir: Option<PathBuf>,
}

#[derive(Clone)]
struct AppState {
    token: String,
    log_dir: PathBuf,
    profile: String,
    full_auto: bool,
    codex_bin: PathBuf,
    recipes_dir: PathBuf,
    recipe_catalog: Arc<RecipeCatalog>,
    com_recipes_dir: PathBuf,
    com_recipe_catalog: Arc<ComRecipeCatalog>,
    sessions: Arc<Mutex<HashMap<String, SessionHandle>>>,
    tasks: Arc<Mutex<HashMap<String, TaskHandle>>>,
}

struct SessionHandle {
    session: AgentSession,
    child: Child,
}

struct TaskHandle {
    result: RemoteAgentTaskResult,
    child: Option<Child>,
    gui_plan: Option<GuiAutomationPlan>,
    com_plan: Option<ComAutomationPlan>,
    input_workdir: Option<PathBuf>,
    stdout_summary: Arc<Mutex<String>>,
    stderr_summary: Arc<Mutex<String>>,
    stdout_as_message: bool,
}

#[derive(Debug, Deserialize)]
struct StartSessionRequest {
    prompt: String,
    cwd: Option<String>,
    profile: Option<String>,
    full_auto: Option<bool>,
}

#[derive(Debug, Serialize)]
struct TaskStartedPayload<'a> {
    status: &'a str,
    kind: &'a str,
    prompt: Option<&'a str>,
    cwd: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct TaskMessagePayload<'a> {
    level: &'a str,
    message: &'a str,
}

#[derive(Debug, Serialize)]
struct GuiAutomationStartedPayload<'a> {
    action: &'a str,
    platform: &'a str,
    engine: &'a str,
    mode: &'a str,
    recipe_id: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct GuiAutomationActionPayload {
    action: String,
    platform: String,
    engine: String,
    mode: String,
    recipe_id: Option<String>,
    exit_code: Option<i32>,
    stderr_summary: String,
    failure_reason: Option<TaskFailureReason>,
}

#[derive(Debug, Serialize)]
struct ComAutomationStartedPayload<'a> {
    action: &'a str,
    recipe_id: Option<&'a str>,
    prog_id: Option<&'a str>,
    visible: bool,
}

#[derive(Debug, Serialize)]
struct ComAutomationActionPayload {
    action: String,
    recipe_id: Option<String>,
    prog_id: Option<String>,
    visible: bool,
    exit_code: Option<i32>,
    stderr_summary: String,
    failure_reason: Option<TaskFailureReason>,
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

#[derive(Debug, Deserialize)]
struct EventsQuery {
    since: Option<u64>,
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

    let recipes_dir = cli.recipes_dir.unwrap_or_else(bundled_recipes_dir);
    let recipe_catalog = RecipeCatalog::load_dir(&recipes_dir).unwrap_or_else(|err| {
        eprintln!(
            "warning: failed to load GUI recipes from {}: {err}; using bundled recipes only",
            recipes_dir.display()
        );
        RecipeCatalog::bundled()
    });

    let com_recipes_dir = cli.com_recipes_dir.unwrap_or_else(bundled_com_recipes_dir);
    let com_recipe_catalog = ComRecipeCatalog::load_dir(&com_recipes_dir).unwrap_or_else(|err| {
        eprintln!(
            "warning: failed to load COM recipes from {}: {err}; using bundled recipes only",
            com_recipes_dir.display()
        );
        ComRecipeCatalog::bundled()
    });

    let state = AppState {
        token: cli.token,
        log_dir: cli.log_dir,
        profile: cli.profile,
        full_auto: cli.full_auto,
        codex_bin,
        recipes_dir,
        recipe_catalog: Arc::new(recipe_catalog),
        com_recipes_dir,
        com_recipe_catalog: Arc::new(com_recipe_catalog),
        sessions: Arc::new(Mutex::new(HashMap::new())),
        tasks: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/capabilities", get(capabilities))
        .route("/sessions", post(start_session).get(list_sessions))
        .route("/sessions/{session_id}", get(get_session))
        .route("/sessions/{session_id}/events", get(read_session_events))
        .route("/sessions/{session_id}/stop", post(stop_session))
        .route("/tasks", post(start_task).get(list_tasks))
        .route("/tasks/{task_id}", get(get_task))
        .route("/tasks/{task_id}/events", get(read_task_events))
        .route("/tasks/{task_id}/cancel", post(cancel_task))
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

async fn capabilities(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    Json(RemoteAgentCapabilities {
        codex: state.codex_bin.is_file(),
        batch: !BatchAdapterRegistry::with_builtin_adapters()
            .capabilities()
            .is_empty(),
        program: true,
        rdp: false,
        gui_automation: true,
        com_automation: com_automation_supported(),
        computer_use: cfg!(any(target_os = "macos", windows)),
        service_control: true,
        platform: std::env::consts::OS.to_string(),
        permission_notes: vec![
            "trusted_full_auto accepts tasks from trusted Wormhole peers".into(),
            "service install/uninstall is explicit and requires OS administrator or user approval".into(),
            "service control status returns JSON; install/start may fail without elevation".into(),
            "GUI automation may require Accessibility/UI Automation permissions".into(),
            "computer_use runs desktop_screenshot / pointer / type_text recipes via wormhole-computer-use".into(),
            "COM automation requires Windows, installed Office for Office recipes, and an interactive desktop session".into(),
            "rdp_session tasks are executed by Wormhole Desktop RDP iroh node, not wormhole-agentd"
                .into(),
        ],
    })
    .into_response()
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
            .unwrap_or_else(|_| {
                error_response(StatusCode::INTERNAL_SERVER_ERROR, "response error")
            }),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            error_response(StatusCode::NOT_FOUND, "log not found")
        }
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string()),
    }
}

async fn start_task(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<RemoteAgentTaskRequest>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    match spawn_task(state.clone(), req).await {
        Ok(result) => Json(result).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string()),
    }
}

async fn list_tasks(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    reap_finished_tasks(&state).await;
    let tasks = state
        .tasks
        .lock()
        .await
        .values()
        .map(|handle| handle.result.clone())
        .collect::<Vec<_>>();
    Json(tasks).into_response()
}

async fn get_task(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(task_id): AxumPath<String>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    reap_finished_tasks(&state).await;
    let tasks = state.tasks.lock().await;
    match tasks.get(&task_id) {
        Some(handle) => Json(handle.result.clone()).into_response(),
        None => error_response(StatusCode::NOT_FOUND, "task not found"),
    }
}

async fn cancel_task(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(task_id): AxumPath<String>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    let mut tasks = state.tasks.lock().await;
    let Some(handle) = tasks.get_mut(&task_id) else {
        return error_response(StatusCode::NOT_FOUND, "task not found");
    };
    if let Some(child) = handle.child.as_mut() {
        let _ = child.kill().await;
    }
    handle.result.status = TaskStatus::Cancelled;
    Json(handle.result.clone()).into_response()
}

async fn read_task_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(task_id): AxumPath<String>,
    Query(query): Query<EventsQuery>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    if !is_safe_session_id(&task_id) {
        return error_response(StatusCode::BAD_REQUEST, "invalid task id");
    }
    let path = state.log_dir.join(format!("{task_id}.jsonl"));
    match fs::read_to_string(path).await {
        Ok(log) => {
            let body = tail_task_audit_ndjson(&log, query.since);
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/x-ndjson")
                .body(Body::from(body))
                .unwrap_or_else(|_| {
                    error_response(StatusCode::INTERNAL_SERVER_ERROR, "response error")
                })
        }
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

    let mut cmd = build_codex_exec_command(
        &state.codex_bin,
        &profile,
        &prompt,
        cwd.as_deref(),
        full_auto,
    )?;
    cmd.stdin(Stdio::null())
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

async fn spawn_task(state: AppState, req: RemoteAgentTaskRequest) -> Result<RemoteAgentTaskResult> {
    if matches!(req.kind, TaskKind::BatchTask { .. }) {
        return spawn_batch_task(state, req).await;
    }

    let (req, input_workdir) = materialize_inline_task_files(state.log_dir.as_path(), req).await?;

    if matches!(req.kind, TaskKind::RdpSession { .. }) {
        let task_id = if req.task_id.trim().is_empty() {
            Uuid::now_v7().to_string()
        } else {
            req.task_id.clone()
        };
        let log_path = state.log_dir.join(format!("{task_id}.jsonl"));
        let cwd = req
            .cwd
            .as_ref()
            .map(|p| p.to_string_lossy().to_string())
            .filter(|v| !v.trim().is_empty());
        write_audit(
            &log_path,
            &AuditEvent {
                timestamp: unix_secs(),
                session_id: &task_id,
                event: "task_started",
                payload: TaskStartedPayload {
                    status: "delegated",
                    kind: task_kind_label(&req.kind),
                    prompt: req.prompt.as_deref(),
                    cwd: cwd.as_deref(),
                },
            },
        )
        .await?;
        let result = rdp_session_delegation_result(&task_id, &log_path);
        write_task_message(
            &log_path,
            &task_id,
            "info",
            "rdp_session delegated to Wormhole Desktop RDP node",
        )
        .await?;
        state.tasks.lock().await.insert(
            task_id.clone(),
            TaskHandle {
                result: result.clone(),
                child: None,
                gui_plan: None,
                com_plan: None,
                input_workdir: None,
                stdout_summary: Arc::new(Mutex::new(String::new())),
                stderr_summary: Arc::new(Mutex::new(String::new())),
                stdout_as_message: false,
            },
        );
        return Ok(result);
    }

    let task_id = if req.task_id.trim().is_empty() {
        Uuid::now_v7().to_string()
    } else {
        req.task_id.clone()
    };
    let log_path = state.log_dir.join(format!("{task_id}.jsonl"));
    let cwd = req
        .cwd
        .as_ref()
        .map(|p| p.to_string_lossy().to_string())
        .filter(|v| !v.trim().is_empty());

    write_audit(
        &log_path,
        &AuditEvent {
            timestamp: unix_secs(),
            session_id: &task_id,
            event: "task_started",
            payload: TaskStartedPayload {
                status: "running",
                kind: task_kind_label(&req.kind),
                prompt: req.prompt.as_deref(),
                cwd: cwd.as_deref(),
            },
        },
    )
    .await?;

    let gui_plan = if matches!(req.kind, TaskKind::GuiAutomation { .. }) {
        match expand_gui_automation(
            &req.kind,
            state.recipe_catalog.as_ref(),
            Some(state.recipes_dir.as_path()),
        ) {
            Ok(plan) => {
                write_gui_automation_started(&log_path, &task_id, &plan).await?;
                if matches!(plan.mode, GuiControlMode::RdpVisible) {
                    write_task_message(
                        &log_path,
                        &task_id,
                        "warning",
                        "rdp_visible mode selected; session binding is not yet enforced",
                    )
                    .await?;
                }
                Some(plan)
            }
            Err(err) => {
                let failure_reason = gui_error_to_failure_reason(&err);
                let result =
                    failed_task_result(&task_id, &log_path, &err.to_string(), failure_reason);
                write_task_message(&log_path, &task_id, "error", &err.to_string()).await?;
                state.tasks.lock().await.insert(
                    task_id.clone(),
                    TaskHandle {
                        result: result.clone(),
                        child: None,
                        gui_plan: None,
                        com_plan: None,
                        input_workdir: input_workdir.clone(),
                        stdout_summary: Arc::new(Mutex::new(String::new())),
                        stderr_summary: Arc::new(Mutex::new(String::new())),
                        stdout_as_message: false,
                    },
                );
                return Ok(result);
            }
        }
    } else {
        None
    };

    let com_plan = if matches!(req.kind, TaskKind::ComAutomation { .. }) {
        match expand_com_automation(
            &req.kind,
            state.com_recipe_catalog.as_ref(),
            Some(state.com_recipes_dir.as_path()),
        ) {
            Ok(plan) => {
                write_com_automation_started(&log_path, &task_id, &plan).await?;
                Some(plan)
            }
            Err(err) => {
                let failure_reason = com_error_to_failure_reason(&err);
                let result =
                    failed_task_result(&task_id, &log_path, &err.to_string(), failure_reason);
                write_task_message(&log_path, &task_id, "error", &err.to_string()).await?;
                state.tasks.lock().await.insert(
                    task_id.clone(),
                    TaskHandle {
                        result: result.clone(),
                        child: None,
                        gui_plan: None,
                        com_plan: None,
                        input_workdir: input_workdir.clone(),
                        stdout_summary: Arc::new(Mutex::new(String::new())),
                        stderr_summary: Arc::new(Mutex::new(String::new())),
                        stdout_as_message: false,
                    },
                );
                return Ok(result);
            }
        }
    } else {
        None
    };

    let mut command = match build_task_command(&state, &req, gui_plan.as_ref(), com_plan.as_ref())?
    {
        Some(command) => command,
        None => {
            let result = unsupported_task_result(
                &task_id,
                &log_path,
                "task kind is not supported by wormhole-agentd",
            );
            write_task_message(&log_path, &task_id, "warning", "task kind is not supported")
                .await?;
            state.tasks.lock().await.insert(
                task_id.clone(),
                TaskHandle {
                    result: result.clone(),
                    child: None,
                    gui_plan: None,
                    com_plan: None,
                    input_workdir: input_workdir.clone(),
                    stdout_summary: Arc::new(Mutex::new(String::new())),
                    stderr_summary: Arc::new(Mutex::new(String::new())),
                    stdout_as_message: false,
                },
            );
            return Ok(result);
        }
    };

    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    if let Some(cwd) = cwd.as_deref() {
        command.current_dir(cwd);
    }
    let mut child = command
        .spawn()
        .with_context(|| format!("failed to spawn task {}", task_id))?;

    let stdout_summary = Arc::new(Mutex::new(String::new()));
    let stderr_summary = Arc::new(Mutex::new(String::new()));
    let stdout_as_message = matches!(req.kind, TaskKind::ProgramTask { .. });
    if let Some(stdout) = child.stdout.take() {
        tokio::spawn(copy_lines_to_audit_with_summary(
            log_path.clone(),
            task_id.clone(),
            "stdout",
            stdout,
            stdout_summary.clone(),
        ));
    }
    if let Some(stderr) = child.stderr.take() {
        if gui_plan.is_some() || com_plan.is_some() {
            tokio::spawn(copy_lines_to_audit_with_summary(
                log_path.clone(),
                task_id.clone(),
                "stderr",
                stderr,
                stderr_summary.clone(),
            ));
        } else {
            tokio::spawn(copy_lines_to_audit(
                log_path.clone(),
                task_id.clone(),
                "stderr",
                stderr,
            ));
        }
    }

    let result = RemoteAgentTaskResult {
        task_id: task_id.clone(),
        status: TaskStatus::Running,
        exit_code: None,
        artifacts: vec![TaskArtifact {
            path: Some(log_path.clone()),
            kind: TaskArtifactKind::Log,
            label: "audit log".into(),
        }],
        changed_files: req
            .files
            .iter()
            .filter(|file| {
                matches!(
                    file.role,
                    agent_core::TaskFileRole::Output | agent_core::TaskFileRole::InOut
                )
            })
            .map(|file| file.path.clone())
            .collect(),
        message: Some("task accepted".into()),
        log_path: Some(log_path),
        failure_reason: None,
    };
    state.tasks.lock().await.insert(
        task_id,
        TaskHandle {
            result: result.clone(),
            child: Some(child),
            gui_plan,
            com_plan,
            input_workdir,
            stdout_summary,
            stderr_summary,
            stdout_as_message,
        },
    );
    Ok(result)
}

async fn spawn_batch_task(
    state: AppState,
    req: RemoteAgentTaskRequest,
) -> Result<RemoteAgentTaskResult> {
    let task_id = if req.task_id.trim().is_empty() {
        Uuid::now_v7().to_string()
    } else {
        req.task_id.clone()
    };
    let log_path = state.log_dir.join(format!("{task_id}.jsonl"));
    let cwd = req
        .cwd
        .as_ref()
        .map(|p| p.to_string_lossy().to_string())
        .filter(|v| !v.trim().is_empty());

    write_audit(
        &log_path,
        &AuditEvent {
            timestamp: unix_secs(),
            session_id: &task_id,
            event: "task_started",
            payload: TaskStartedPayload {
                status: "running",
                kind: "batch_task",
                prompt: req.prompt.as_deref(),
                cwd: cwd.as_deref(),
            },
        },
    )
    .await?;

    let batch_request = match batch_request_from_remote_task(&req, None) {
        Ok(request) => request,
        Err(message) => {
            let result = unsupported_task_result(&task_id, &log_path, &message);
            write_task_message(&log_path, &task_id, "error", &message).await?;
            state.tasks.lock().await.insert(
                task_id.clone(),
                TaskHandle {
                    result: result.clone(),
                    child: None,
                    gui_plan: None,
                    com_plan: None,
                    input_workdir: None,
                    stdout_summary: Arc::new(Mutex::new(String::new())),
                    stderr_summary: Arc::new(Mutex::new(String::new())),
                    stdout_as_message: false,
                },
            );
            return Ok(result);
        }
    };

    let running = RemoteAgentTaskResult {
        task_id: task_id.clone(),
        status: TaskStatus::Running,
        exit_code: None,
        artifacts: vec![TaskArtifact {
            path: Some(log_path.clone()),
            kind: TaskArtifactKind::Log,
            label: "audit log".into(),
        }],
        changed_files: req
            .files
            .iter()
            .filter(|file| {
                matches!(
                    file.role,
                    agent_core::TaskFileRole::Output | agent_core::TaskFileRole::InOut
                )
            })
            .map(|file| file.path.clone())
            .collect(),
        message: Some("batch task accepted".into()),
        log_path: Some(log_path.clone()),
        failure_reason: None,
    };
    state.tasks.lock().await.insert(
        task_id.clone(),
        TaskHandle {
            result: running.clone(),
            child: None,
            gui_plan: None,
            com_plan: None,
            input_workdir: None,
            stdout_summary: Arc::new(Mutex::new(String::new())),
            stderr_summary: Arc::new(Mutex::new(String::new())),
            stdout_as_message: false,
        },
    );

    let final_result = match tokio::task::spawn_blocking(move || {
        BatchAdapterRegistry::with_builtin_adapters().execute(&batch_request)
    })
    .await
    {
        Ok(Ok(batch)) => remote_task_result_from_batch(&task_id, &batch, Some(log_path.clone())),
        Ok(Err(err)) => unsupported_task_result(&task_id, &log_path, &err.to_string()),
        Err(err) => unsupported_task_result(
            &task_id,
            &log_path,
            &format!("batch task worker panicked: {err}"),
        ),
    };

    if let Some(message) = final_result.message.as_deref() {
        let level = if matches!(final_result.status, TaskStatus::Completed) {
            "info"
        } else {
            "error"
        };
        write_task_message(&log_path, &task_id, level, message).await?;
    }
    write_audit(
        &log_path,
        &AuditEvent {
            timestamp: unix_secs(),
            session_id: &task_id,
            event: "task_ended",
            payload: SessionEndedPayload {
                status: format!("{:?}", final_result.status),
                exit_code: final_result.exit_code,
            },
        },
    )
    .await?;

    if let Some(handle) = state.tasks.lock().await.get_mut(&task_id) {
        handle.result = final_result.clone();
    }
    Ok(final_result)
}

fn build_task_command(
    state: &AppState,
    req: &RemoteAgentTaskRequest,
    gui_plan: Option<&GuiAutomationPlan>,
    com_plan: Option<&ComAutomationPlan>,
) -> Result<Option<Command>> {
    match &req.kind {
        TaskKind::CodexFileTask => {
            let prompt = req
                .prompt
                .as_deref()
                .unwrap_or("Complete the requested Wormhole remote file task.");
            let cwd = req
                .cwd
                .as_ref()
                .map(|p| p.to_string_lossy())
                .filter(|v| !v.trim().is_empty());
            let mut cmd = build_codex_exec_command(
                &state.codex_bin,
                &state.profile,
                prompt,
                cwd.as_deref(),
                state.full_auto,
            )?;
            cmd.stdin(Stdio::null());
            Ok(Some(cmd))
        }
        TaskKind::ProgramTask { program, args } => {
            let program_path = resolve_program_task_binary(program);
            let mut cmd = Command::new(program_path);
            cmd.args(args).stdin(Stdio::null());
            for (key, value) in &req.env {
                cmd.env(key, value);
            }
            Ok(Some(cmd))
        }
        TaskKind::GuiAutomation { .. } => {
            let plan = gui_plan
                .ok_or_else(|| anyhow!("gui automation plan missing after recipe expansion"))?;
            Ok(Some(gui_automation_command(plan)))
        }
        TaskKind::ComAutomation { .. } => {
            let plan = com_plan
                .ok_or_else(|| anyhow!("com automation plan missing after recipe expansion"))?;
            Ok(Some(com_automation_command(plan)))
        }
        TaskKind::ServiceControl { action } => {
            service_control::service_control_command(&state.log_dir, &state.codex_bin, action)
                .map(Some)
        }
        TaskKind::BatchTask { .. } => Ok(None),
        TaskKind::RdpSession { .. } => Ok(None),
    }
}

async fn materialize_inline_task_files(
    log_dir: &Path,
    mut req: RemoteAgentTaskRequest,
) -> Result<(RemoteAgentTaskRequest, Option<PathBuf>)> {
    let has_inline_files = req
        .files
        .iter()
        .any(|file| file.inline_bytes_base64.is_some());
    if !has_inline_files {
        return Ok((req, None));
    }

    let task_id = if req.task_id.trim().is_empty() {
        Uuid::now_v7().to_string()
    } else {
        req.task_id.clone()
    };
    if req.task_id.trim().is_empty() {
        req.task_id = task_id.clone();
    }
    let workdir = log_dir.join("task-inputs").join(&task_id);
    fs::create_dir_all(&workdir)
        .await
        .with_context(|| format!("create inline task input dir {}", workdir.display()))?;

    for file in &mut req.files {
        let Some(encoded) = file.inline_bytes_base64.take() else {
            continue;
        };
        if !matches!(
            file.role,
            agent_core::TaskFileRole::Input | agent_core::TaskFileRole::InOut
        ) {
            anyhow::bail!(
                "inline task file {} must be input or in_out",
                file.path.display()
            );
        }
        let relative = safe_inline_task_file_path(&file.path)?;
        let data = BASE64
            .decode(encoded.as_bytes())
            .with_context(|| format!("decode inline task file {}", file.path.display()))?;
        if let Some(expected) = file.inline_sha256.as_deref() {
            let actual = format!("{:x}", Sha256::digest(&data));
            if !actual.eq_ignore_ascii_case(expected) {
                anyhow::bail!(
                    "inline task file {} sha256 mismatch: expected {}, got {}",
                    file.path.display(),
                    expected,
                    actual
                );
            }
        }
        let target = workdir.join(&relative);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create inline task file dir {}", parent.display()))?;
        }
        fs::write(&target, data)
            .await
            .with_context(|| format!("write inline task file {}", target.display()))?;
        file.path = relative;
    }

    if req.cwd.is_none() {
        req.cwd = Some(workdir.clone());
    }
    Ok((req, Some(workdir)))
}

fn safe_inline_task_file_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        anyhow::bail!("inline task file path must be relative: {}", path.display());
    }
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Normal(part) => out.push(part),
            std::path::Component::CurDir => {}
            _ => anyhow::bail!("inline task file path is unsafe: {}", path.display()),
        }
    }
    if out.as_os_str().is_empty() {
        anyhow::bail!("inline task file path cannot be empty");
    }
    Ok(out)
}

fn gui_automation_command(plan: &GuiAutomationPlan) -> Command {
    match plan.engine {
        GuiEngine::Powershell => {
            let mut cmd = Command::new("powershell");
            cmd.args([
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                &plan.script,
            ]);
            cmd
        }
        GuiEngine::Osascript => {
            let mut cmd = Command::new("osascript");
            cmd.args(["-e", &plan.script]);
            cmd
        }
        GuiEngine::Shell => {
            let mut cmd = Command::new("sh");
            cmd.args(["-lc", &plan.script]);
            cmd
        }
    }
}

fn com_automation_command(plan: &ComAutomationPlan) -> Command {
    let mut cmd = Command::new("powershell");
    cmd.args([
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        &plan.script,
    ]);
    cmd
}

async fn reap_finished_tasks(state: &AppState) {
    struct EndedTask {
        task_id: String,
        result: RemoteAgentTaskResult,
        gui_plan: Option<GuiAutomationPlan>,
        com_plan: Option<ComAutomationPlan>,
        input_workdir: Option<PathBuf>,
        stderr_summary: String,
    }

    let mut ended = Vec::new();
    {
        let mut tasks = state.tasks.lock().await;
        for (id, handle) in tasks.iter_mut() {
            if !matches!(
                handle.result.status,
                TaskStatus::Running | TaskStatus::Accepted
            ) {
                continue;
            }
            let Some(child) = handle.child.as_mut() else {
                continue;
            };
            match child.try_wait() {
                Ok(Some(status)) => {
                    handle.result.exit_code = status.code();
                    let stderr_summary = handle.stderr_summary.lock().await.clone();
                    if handle.gui_plan.is_some() {
                        if !status.success() {
                            handle.result.failure_reason =
                                Some(classify_gui_failure(status.code(), &stderr_summary));
                        }
                    } else if handle.com_plan.is_some() {
                        if !status.success() {
                            handle.result.failure_reason =
                                Some(classify_com_failure(status.code(), &stderr_summary));
                        }
                    }
                    handle.result.status = if status.success() {
                        TaskStatus::Completed
                    } else {
                        TaskStatus::Failed
                    };
                    let stdout_summary = handle.stdout_summary.lock().await.clone();
                    handle.result.message = if status.success()
                        && handle.stdout_as_message
                        && !stdout_summary.trim().is_empty()
                    {
                        Some(summarize_stderr(&stdout_summary, 8192))
                    } else {
                        Some(handle.result.status_text().to_string())
                    };
                    ended.push(EndedTask {
                        task_id: id.clone(),
                        result: handle.result.clone(),
                        gui_plan: handle.gui_plan.clone(),
                        com_plan: handle.com_plan.clone(),
                        input_workdir: handle.input_workdir.take(),
                        stderr_summary,
                    });
                }
                Ok(None) => {}
                Err(err) => {
                    handle.result.status = TaskStatus::Failed;
                    handle.result.failure_reason = Some(TaskFailureReason::SpawnError);
                    handle.result.message = Some(err.to_string());
                    ended.push(EndedTask {
                        task_id: id.clone(),
                        result: handle.result.clone(),
                        gui_plan: handle.gui_plan.clone(),
                        com_plan: handle.com_plan.clone(),
                        input_workdir: handle.input_workdir.take(),
                        stderr_summary: handle.stderr_summary.lock().await.clone(),
                    });
                }
            }
        }
    }
    for ended in ended {
        if let Some(log_path) = ended.result.log_path.as_deref() {
            let _ = write_audit(
                log_path,
                &AuditEvent {
                    timestamp: unix_secs(),
                    session_id: &ended.task_id,
                    event: "task_ended",
                    payload: SessionEndedPayload {
                        status: format!("{:?}", ended.result.status),
                        exit_code: ended.result.exit_code,
                    },
                },
            )
            .await;
            if let Some(plan) = ended.gui_plan.as_ref() {
                let stderr_summary = summarize_stderr(&ended.stderr_summary, 512);
                let failure_reason = if ended.result.status == TaskStatus::Failed {
                    ended.result.failure_reason.clone()
                } else {
                    None
                };
                let _ = write_gui_automation_action(
                    log_path,
                    &ended.task_id,
                    plan,
                    ended.result.exit_code,
                    &stderr_summary,
                    failure_reason,
                )
                .await;
            }
            if let Some(plan) = ended.com_plan.as_ref() {
                let stderr_summary = summarize_stderr(&ended.stderr_summary, 512);
                let failure_reason = if ended.result.status == TaskStatus::Failed {
                    ended.result.failure_reason.clone()
                } else {
                    None
                };
                let _ = write_com_automation_action(
                    log_path,
                    &ended.task_id,
                    plan,
                    ended.result.exit_code,
                    &stderr_summary,
                    failure_reason,
                )
                .await;
            }
        }
        if let Some(input_workdir) = ended.input_workdir.as_ref() {
            let _ = fs::remove_dir_all(input_workdir).await;
        }
    }
}

trait TaskStatusText {
    fn status_text(&self) -> &'static str;
}

impl TaskStatusText for RemoteAgentTaskResult {
    fn status_text(&self) -> &'static str {
        match self.status {
            TaskStatus::Completed => "task completed",
            TaskStatus::Failed => "task failed",
            TaskStatus::Cancelled => "task cancelled",
            TaskStatus::PermissionRequired => "permission required",
            TaskStatus::Unsupported => "unsupported task",
            TaskStatus::TimedOut => "task timed out",
            TaskStatus::Accepted | TaskStatus::Running => "task running",
        }
    }
}

fn rdp_session_delegation_result(task_id: &str, log_path: &Path) -> RemoteAgentTaskResult {
    RemoteAgentTaskResult {
        task_id: task_id.to_string(),
        status: TaskStatus::Unsupported,
        exit_code: None,
        artifacts: vec![TaskArtifact {
            path: Some(log_path.to_path_buf()),
            kind: TaskArtifactKind::Log,
            label: "audit log".into(),
        }],
        changed_files: Vec::new(),
        message: Some(
            "rdp_session is handled by Wormhole Desktop (remote_agent_submit_task / RDP iroh node); \
             use Agent Mode on the desktop host with iroh-transport+rdp features"
                .into(),
        ),
        log_path: Some(log_path.to_path_buf()),
        failure_reason: None,
    }
}

fn unsupported_task_result(task_id: &str, log_path: &Path, message: &str) -> RemoteAgentTaskResult {
    RemoteAgentTaskResult {
        task_id: task_id.to_string(),
        status: TaskStatus::Unsupported,
        exit_code: None,
        artifacts: vec![TaskArtifact {
            path: Some(log_path.to_path_buf()),
            kind: TaskArtifactKind::Log,
            label: "audit log".into(),
        }],
        changed_files: Vec::new(),
        message: Some(message.to_string()),
        log_path: Some(log_path.to_path_buf()),
        failure_reason: None,
    }
}

fn failed_task_result(
    task_id: &str,
    log_path: &Path,
    message: &str,
    failure_reason: TaskFailureReason,
) -> RemoteAgentTaskResult {
    RemoteAgentTaskResult {
        task_id: task_id.to_string(),
        status: TaskStatus::Failed,
        exit_code: None,
        artifacts: vec![TaskArtifact {
            path: Some(log_path.to_path_buf()),
            kind: TaskArtifactKind::Log,
            label: "audit log".into(),
        }],
        changed_files: Vec::new(),
        message: Some(message.to_string()),
        log_path: Some(log_path.to_path_buf()),
        failure_reason: Some(failure_reason),
    }
}

fn gui_error_to_failure_reason(err: &GuiAutomationError) -> TaskFailureReason {
    match err {
        GuiAutomationError::RecipeNotFound(_) => TaskFailureReason::RecipeNotFound,
        GuiAutomationError::PlatformUnsupported { .. } => TaskFailureReason::PlatformUnsupported,
        GuiAutomationError::MissingParam(_) => TaskFailureReason::InvalidParameters,
        GuiAutomationError::MissingInput => TaskFailureReason::InvalidParameters,
        GuiAutomationError::ReadRecipe { .. } | GuiAutomationError::InvalidDescriptor(_) => {
            TaskFailureReason::ScriptError
        }
    }
}

fn com_error_to_failure_reason(err: &ComAutomationError) -> TaskFailureReason {
    match err {
        ComAutomationError::RecipeNotFound(_) => TaskFailureReason::RecipeNotFound,
        ComAutomationError::PlatformUnsupported { .. } | ComAutomationError::NotWindows => {
            TaskFailureReason::PlatformUnsupported
        }
        ComAutomationError::MissingParam(_) | ComAutomationError::MissingInput => {
            TaskFailureReason::InvalidParameters
        }
        ComAutomationError::ReadRecipe { .. } | ComAutomationError::InvalidDescriptor(_) => {
            TaskFailureReason::ScriptError
        }
    }
}

fn gui_control_mode_label(mode: &GuiControlMode) -> &'static str {
    match mode {
        GuiControlMode::RdpVisible => "rdp_visible",
        GuiControlMode::BackgroundAutomation => "background_automation",
        GuiControlMode::ComputerUse => "computer_use",
    }
}

fn codex_exec_sandbox(prompt: &str, full_auto: bool) -> CodexExecSandbox {
    let computer_use_enabled = std::env::var("WORMHOLE_COMPUTER_USE_ENABLED")
        .ok()
        .is_some_and(|v| v == "1");
    resolve_codex_exec_sandbox(CodexExecPolicyInput {
        computer_use_enabled,
        prompt,
        force_full_auto: full_auto,
    })
}

fn build_codex_exec_command(
    codex_bin: &Path,
    profile: &str,
    prompt: &str,
    cwd: Option<&str>,
    full_auto: bool,
) -> Result<Command> {
    let argv = codex_exec_argv(&CodexExecTurn {
        profile,
        message: prompt,
        thread_id: None,
        cwd,
        sandbox: codex_exec_sandbox(prompt, full_auto),
    })
    .map_err(|err| anyhow!(err))?;
    let mut cmd = Command::new(codex_bin);
    cmd.args(argv);
    Ok(cmd)
}

fn engine_label(engine: &GuiEngine) -> &'static str {
    match engine {
        GuiEngine::Powershell => "powershell",
        GuiEngine::Osascript => "osascript",
        GuiEngine::Shell => "shell",
    }
}

async fn write_gui_automation_started(
    log_path: &Path,
    task_id: &str,
    plan: &GuiAutomationPlan,
) -> Result<()> {
    write_audit(
        log_path,
        &AuditEvent {
            timestamp: unix_secs(),
            session_id: task_id,
            event: "gui_automation_started",
            payload: GuiAutomationStartedPayload {
                action: &plan.action,
                platform: &plan.platform,
                engine: engine_label(&plan.engine),
                mode: gui_control_mode_label(&plan.mode),
                recipe_id: plan.recipe_id.as_deref(),
            },
        },
    )
    .await
}

async fn write_gui_automation_action(
    log_path: &Path,
    task_id: &str,
    plan: &GuiAutomationPlan,
    exit_code: Option<i32>,
    stderr_summary: &str,
    failure_reason: Option<TaskFailureReason>,
) -> Result<()> {
    write_audit(
        log_path,
        &AuditEvent {
            timestamp: unix_secs(),
            session_id: task_id,
            event: "gui_automation_action",
            payload: GuiAutomationActionPayload {
                action: plan.action.clone(),
                platform: plan.platform.clone(),
                engine: engine_label(&plan.engine).to_string(),
                mode: gui_control_mode_label(&plan.mode).to_string(),
                recipe_id: plan.recipe_id.clone(),
                exit_code,
                stderr_summary: stderr_summary.to_string(),
                failure_reason,
            },
        },
    )
    .await
}

async fn write_com_automation_started(
    log_path: &Path,
    task_id: &str,
    plan: &ComAutomationPlan,
) -> Result<()> {
    write_audit(
        log_path,
        &AuditEvent {
            timestamp: unix_secs(),
            session_id: task_id,
            event: "com_automation_started",
            payload: ComAutomationStartedPayload {
                action: &plan.action,
                recipe_id: plan.recipe_id.as_deref(),
                prog_id: plan.prog_id.as_deref(),
                visible: plan.visible,
            },
        },
    )
    .await
}

async fn write_com_automation_action(
    log_path: &Path,
    task_id: &str,
    plan: &ComAutomationPlan,
    exit_code: Option<i32>,
    stderr_summary: &str,
    failure_reason: Option<TaskFailureReason>,
) -> Result<()> {
    write_audit(
        log_path,
        &AuditEvent {
            timestamp: unix_secs(),
            session_id: task_id,
            event: "com_automation_action",
            payload: ComAutomationActionPayload {
                action: plan.action.clone(),
                recipe_id: plan.recipe_id.clone(),
                prog_id: plan.prog_id.clone(),
                visible: plan.visible,
                exit_code,
                stderr_summary: stderr_summary.to_string(),
                failure_reason,
            },
        },
    )
    .await
}

async fn write_task_message(
    log_path: &Path,
    task_id: &str,
    level: &'static str,
    message: &str,
) -> Result<()> {
    write_audit(
        log_path,
        &AuditEvent {
            timestamp: unix_secs(),
            session_id: task_id,
            event: "task_event",
            payload: TaskMessagePayload { level, message },
        },
    )
    .await
}

fn task_kind_label(kind: &TaskKind) -> &'static str {
    match kind {
        TaskKind::CodexFileTask => "codex_file_task",
        TaskKind::BatchTask { .. } => "batch_task",
        TaskKind::ProgramTask { .. } => "program_task",
        TaskKind::RdpSession { .. } => "rdp_session",
        TaskKind::GuiAutomation { .. } => "gui_automation",
        TaskKind::ComAutomation { .. } => "com_automation",
        TaskKind::ServiceControl { .. } => "service_control",
    }
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

async fn copy_lines_to_audit_with_summary<R>(
    log_path: PathBuf,
    session_id: String,
    stream: &'static str,
    reader: R,
    summary: Arc<Mutex<String>>,
) where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if stream == "stderr" {
                    let mut guard = summary.lock().await;
                    if !guard.is_empty() {
                        guard.push('\n');
                    }
                    guard.push_str(&line);
                }
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
                if stream == "stderr" {
                    let mut guard = summary.lock().await;
                    if !guard.is_empty() {
                        guard.push('\n');
                    }
                    guard.push_str(&message);
                }
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

async fn copy_lines_to_audit<R>(
    log_path: PathBuf,
    session_id: String,
    stream: &'static str,
    reader: R,
) where
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
        return Err(error_response(
            StatusCode::UNAUTHORIZED,
            "missing bearer token",
        ));
    };
    let expected = format!("Bearer {}", state.token);
    if value != expected {
        return Err(error_response(
            StatusCode::FORBIDDEN,
            "invalid bearer token",
        ));
    }
    Ok(())
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

fn find_codex_binary() -> Result<PathBuf> {
    for key in ["WORMHOLE_CODEX_BIN", "WORMHOLE_CODEX"] {
        if let Ok(path) = std::env::var(key) {
            let path = PathBuf::from(path);
            if path.is_file() {
                return Ok(path);
            }
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

fn resolve_program_task_binary(program: &str) -> PathBuf {
    let requested = PathBuf::from(program);
    if requested.is_absolute()
        || requested.components().count() > 1
        || program.contains('/')
        || program.contains('\\')
    {
        return requested;
    }
    for dir in program_task_candidate_dirs() {
        if let Some(path) = find_program_in_dir(&dir, program) {
            return path;
        }
    }
    find_in_path(program).unwrap_or(requested)
}

fn program_task_candidate_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Ok(current) = std::env::current_exe()
        && let Some(dir) = current.parent()
    {
        dirs.push(dir.to_path_buf());
        dirs.push(dir.join("../Resources"));
        dirs.push(dir.join("../resources"));
    }
    dirs
}

fn find_program_in_dir(dir: &Path, program: &str) -> Option<PathBuf> {
    let direct = dir.join(program);
    if direct.is_file() {
        return Some(direct);
    }
    let requested = Path::new(program);
    let stem = requested
        .file_stem()
        .or_else(|| requested.file_name())
        .and_then(|value| value.to_str())
        .unwrap_or(program);
    let expected_prefix = format!("{stem}-");
    let expected_extension = requested.extension().and_then(|value| value.to_str());
    let entries = std::fs::read_dir(dir).ok()?;
    entries.filter_map(|entry| entry.ok()).find_map(|entry| {
        let path = entry.path();
        if !path.is_file() {
            return None;
        }
        let file_name = path.file_name()?.to_str()?;
        if !file_name.starts_with(&expected_prefix) {
            return None;
        }
        let extension_matches = match expected_extension {
            Some(extension) => path
                .extension()
                .and_then(|value| value.to_str())
                .map(|candidate| candidate.eq_ignore_ascii_case(extension))
                .unwrap_or(false),
            None => path.extension().is_none(),
        };
        extension_matches.then_some(path)
    })
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
    use super::{codex_exec_sandbox, gui_control_mode_label, is_safe_session_id};
    use agent_core::{codex_exec_argv, CodexExecTurn, GuiControlMode, tail_task_audit_ndjson};
    use compute_core::BatchAdapterRegistry;

    #[test]
    fn batch_capabilities_available_when_adapters_exist() {
        assert!(
            !BatchAdapterRegistry::with_builtin_adapters()
                .capabilities()
                .is_empty()
        );
    }

    #[test]
    fn safe_session_id_rejects_paths() {
        assert!(is_safe_session_id("019ab778-a7fd-7121-9def-0cc725522e55"));
        assert!(!is_safe_session_id("../secret"));
        assert!(!is_safe_session_id("a.jsonl"));
        assert!(!is_safe_session_id(""));
    }

    #[test]
    fn gui_control_mode_includes_computer_use() {
        assert_eq!(
            gui_control_mode_label(&GuiControlMode::ComputerUse),
            "computer_use"
        );
    }

    #[test]
    fn codex_exec_argv_full_auto_when_forced() {
        let argv = codex_exec_argv(&CodexExecTurn {
            profile: "wormhole",
            message: "open calculator",
            thread_id: None,
            cwd: Some("/tmp"),
            sandbox: codex_exec_sandbox("task", true),
        })
        .expect("argv");
        assert!(argv.iter().any(|a| a == "exec"));
        assert!(argv.iter().any(|a| a == "--profile"));
        assert!(
            argv.iter()
                .any(|a| a == "--dangerously-bypass-approvals-and-sandbox")
        );
    }

    #[test]
    fn read_task_events_since_returns_tail_only() {
        let ndjson = "\
{\"timestamp\":1,\"session_id\":\"t1\",\"event\":\"task_started\",\"kind\":\"program_task\"}\n\
{\"timestamp\":2,\"session_id\":\"t1\",\"event\":\"task_event\",\"level\":\"info\",\"message\":\"a\"}\n";
        let tail = tail_task_audit_ndjson(ndjson, Some(1));
        assert_eq!(tail.matches('\n').count(), 1);
        assert!(tail.contains("\"timestamp\":2"));
    }
}
