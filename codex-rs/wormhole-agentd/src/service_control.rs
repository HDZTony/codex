use std::path::{Path, PathBuf};
use std::process::Stdio;

use agent_core::ServiceControlAction;
use anyhow::{Context, Result, anyhow};
use tokio::process::Command;

const SERVICE_TEMPLATE_REL: &str = "apps/desktop/installer/agent-service";

pub fn service_control_command(
    log_dir: &Path,
    codex_bin: &Path,
    action: &ServiceControlAction,
) -> Result<Command> {
    let script = resolve_service_script()?;
    let agentd_bin = std::env::current_exe().context("failed to resolve wormhole-agentd path")?;
    let data_dir = derive_data_dir(log_dir)?;

    let action_name = action_name(action);
    let mut cmd = platform_runner(&script, action_name)?;
    cmd.env("WORMHOLE_AGENTD_BIN", &agentd_bin);
    cmd.env("WORMHOLE_AGENT_DATA_DIR", &data_dir);
    cmd.env("WORMHOLE_CODEX_BIN", codex_bin);
    cmd.stdin(Stdio::null());
    Ok(cmd)
}

pub fn derive_data_dir(log_dir: &Path) -> Result<PathBuf> {
    let agent_dir = log_dir
        .parent()
        .ok_or_else(|| anyhow!("invalid agent log dir: {}", log_dir.display()))?;
    agent_dir
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow!("invalid agent log dir: {}", log_dir.display()))
}

pub fn resolve_service_script() -> Result<PathBuf> {
    if let Ok(dir) = std::env::var("WORMHOLE_AGENT_SERVICE_DIR") {
        let script = platform_script_path(Path::new(&dir));
        if script.is_file() {
            return Ok(script);
        }
        return Err(anyhow!(
            "WORMHOLE_AGENT_SERVICE_DIR is set but {} was not found",
            script.display()
        ));
    }

    let exe = std::env::current_exe().context("failed to resolve current executable")?;
    let Some(parent) = exe.parent() else {
        return Err(anyhow!("executable has no parent directory"));
    };

    for rel in ["agent-service", "installer/agent-service"] {
        let script = platform_script_path(&parent.join(rel));
        if script.is_file() {
            return Ok(script);
        }
    }

    let mut cursor = parent.to_path_buf();
    for _ in 0..10 {
        let script = platform_script_path(&cursor.join(SERVICE_TEMPLATE_REL));
        if script.is_file() {
            return Ok(script);
        }
        if !cursor.pop() {
            break;
        }
    }

    Err(anyhow!(
        "service templates not found; set WORMHOLE_AGENT_SERVICE_DIR or install files from apps/desktop/installer/agent-service"
    ))
}

#[cfg(windows)]
fn platform_script_path(base: &Path) -> PathBuf {
    base.join("windows").join("manage.ps1")
}

#[cfg(target_os = "macos")]
fn platform_script_path(base: &Path) -> PathBuf {
    base.join("macos").join("manage.sh")
}

#[cfg(all(unix, not(target_os = "macos")))]
fn platform_script_path(base: &Path) -> PathBuf {
    base.join("linux").join("manage.sh")
}

#[cfg(windows)]
fn platform_runner(script: &Path, action: &str) -> Result<Command> {
    let mut cmd = Command::new("powershell");
    cmd.args([
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        &script.to_string_lossy(),
        action,
    ]);
    Ok(cmd)
}

#[cfg(unix)]
fn platform_runner(script: &Path, action: &str) -> Result<Command> {
    let mut cmd = Command::new("bash");
    cmd.arg(script).arg(action);
    Ok(cmd)
}

fn action_name(action: &ServiceControlAction) -> &'static str {
    match action {
        ServiceControlAction::Install => "install",
        ServiceControlAction::Uninstall => "uninstall",
        ServiceControlAction::Start => "start",
        ServiceControlAction::Stop => "stop",
        ServiceControlAction::Status => "status",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_data_dir_from_log_dir() {
        let log_dir = PathBuf::from("/tmp/wormhole/agent/logs");
        assert_eq!(
            derive_data_dir(&log_dir).unwrap(),
            PathBuf::from("/tmp/wormhole")
        );
    }

    #[test]
    fn action_names_are_stable() {
        assert_eq!(action_name(&ServiceControlAction::Status), "status");
        assert_eq!(action_name(&ServiceControlAction::Install), "install");
    }

    #[test]
    fn resolve_service_script_from_repo_layout() {
        if std::env::var("WORMHOLE_AGENT_SERVICE_DIR").is_ok() {
            return;
        }
        let script = resolve_service_script().expect("repo templates should be discoverable");
        assert!(script.is_file(), "{}", script.display());
    }
}
