# wormhole-agentd

`wormhole-agentd` is the Wormhole fork integration daemon for Codex.

It exposes the local HTTP contract expected by Wormhole Desktop:

- `GET /health`
- `POST /sessions`
- `GET /sessions`
- `GET /sessions/{id}`
- `GET /sessions/{id}/events`
- `POST /sessions/{id}/stop`

Each session launches the sibling `codex` binary in non-interactive `exec` mode
with JSONL output and the full-access automation flags selected by Wormhole:

```text
codex exec --json --dangerously-bypass-approvals-and-sandbox --skip-git-repo-check ...
```

The daemon writes JSONL audit logs into the `--log-dir` supplied by Wormhole.
It does not install persistence, hide itself, bypass UAC, or alter security
software. It is intended to be launched and stopped by Wormhole Desktop.
