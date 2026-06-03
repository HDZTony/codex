# wormhole-agentd

`wormhole-agentd` is the Wormhole fork integration daemon for Codex.

It exposes the local HTTP contract expected by Wormhole Desktop:

- `GET /health`
- `GET /capabilities`
- `POST /sessions`
- `GET /sessions`
- `GET /sessions/{id}`
- `GET /sessions/{id}/events`
- `POST /sessions/{id}/stop`
- `POST /tasks`
- `GET /tasks`
- `GET /tasks/{id}`
- `GET /tasks/{id}/events`
- `POST /tasks/{id}/cancel`

Each session launches the sibling `codex` binary in non-interactive `exec` mode
with JSONL output and the full-access automation flags selected by Wormhole:

```text
codex exec --json --dangerously-bypass-approvals-and-sandbox --skip-git-repo-check ...
```

The daemon writes JSONL audit logs into the `--log-dir` supplied by Wormhole.
It does not install persistence, hide itself, bypass UAC, or alter security
software. By default Wormhole Desktop launches and stops the daemon for the
current user session only.

The `/tasks` API accepts `agent-core::RemoteAgentTaskRequest`. It supports Codex
file tasks, explicit program tasks, platform GUI automation scripts,
Windows `com_automation` recipes (PowerShell COM; see `docs/com_automation.md` in the
Wormhole repo), `service_control` tasks, and `batch_task` (via
`compute-core::BatchAdapterRegistry`).
`rdp_session` tasks are **not** executed here: Wormhole Desktop handles them on the RDP
iroh node (`remote_agent_submit_task` / P2P agent ALPN). Direct HTTP `POST /tasks` with
`rdp_session` returns `unsupported` plus a delegation message in the audit log.

`GET /capabilities` reports `batch: true` when built-in compute adapters are
available on the host.

## Service mode (explicit install only)

Persistent background operation is **opt-in** through visible OS service managers.
Remote `service_control` tasks invoke the templates under
`apps/desktop/installer/agent-service/`; they never silently register startup
items.

| Platform | Mechanism | Install scope |
| --- | --- | --- |
| Windows | `WormholeAgentd` Windows Service (`sc.exe`, manual start) | Administrator elevation |
| macOS | `com.wormhole.agentd` LaunchAgent in `~/Library/LaunchAgents` | Interactive user |
| Linux | `wormhole-agentd.service` systemd user unit | Interactive user (`systemctl --user`) |

Template resolution order:

1. `WORMHOLE_AGENT_SERVICE_DIR` (directory containing `windows/`, `macos/`, or `linux/`)
2. `{exe_dir}/agent-service/` or `{exe_dir}/installer/agent-service/`
3. Walk upward from the executable for `apps/desktop/installer/agent-service`

### `service_control` actions

| Action | Behavior |
| --- | --- |
| `status` | Prints one JSON line with `installed`, `running`, `message` |
| `install` | Registers the unit/service; does **not** auto-start (Windows: `start=demand`) |
| `uninstall` | Stops and removes registration plus generated wrapper/plist/unit files |
| `start` / `stop` | Delegates to the OS service manager; may fail without permission |

Status JSON example:

```json
{"service_name":"WormholeAgentd","platform":"windows","action":"status","installed":false,"running":false,"message":"service not installed","details":{}}
```

Exit code `2` from the platform script means OS permission/elevation is required.
Task audit logs capture stdout/stderr from the manager script.

### Manual install

See `apps/desktop/installer/agent-service/README.md` for copy-paste commands per
platform. Uninstall uses the same manager script with the `uninstall` action.

Service token persistence: `{data_dir}/agent-service-token` (generated on install).
Audit logs remain under `{data_dir}/agent/logs` after uninstall.
