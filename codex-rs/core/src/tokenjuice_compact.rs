//! Compact Codex tool results before they enter model context (TokenJuice).

use std::path::PathBuf;
use std::sync::Once;

use codex_protocol::models::FunctionCallOutputBody;
use codex_protocol::models::FunctionCallOutputContentItem;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::ResponseInputItem;
use tracing::debug;

use wormhole_tokenjuice::AgentTokenjuiceCompression;
use wormhole_tokenjuice::compact_output_with_policy;
use wormhole_tokenjuice::install_from_data_dir;
use wormhole_tokenjuice::is_recovery_tool;

/// Ensure process-global TokenJuice options + CCR are installed once.
///
/// Resolves data dir from `WORMHOLE_DATA_DIR`, else parent of `CODEX_HOME`,
/// else a temp fallback (in-memory CCR only).
pub(crate) fn ensure_tokenjuice_installed() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let data_dir = resolve_wormhole_data_dir();
        let _ = install_from_data_dir(&data_dir, None, 5.0);
        debug!(path = %data_dir.display(), "[tokenjuice] installed from data dir");
    });
}

fn resolve_wormhole_data_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("WORMHOLE_DATA_DIR") {
        let p = PathBuf::from(dir.trim());
        if !p.as_os_str().is_empty() {
            return p;
        }
    }
    if let Ok(home) = std::env::var("CODEX_HOME") {
        let home = PathBuf::from(home.trim());
        if let Some(parent) = home.parent() {
            return parent.to_path_buf();
        }
    }
    std::env::temp_dir().join("wormhole-tokenjuice")
}

/// Compact tool-result text in a [`ResponseInputItem`] when TokenJuice is enabled.
///
/// Recovery tools are never re-compacted. Failures leave the original text.
pub(crate) async fn compact_response_input_item(
    item: ResponseInputItem,
    tool_name: &str,
) -> ResponseInputItem {
    ensure_tokenjuice_installed();
    if is_recovery_tool(tool_name) {
        return item;
    }
    match item {
        ResponseInputItem::FunctionCallOutput { call_id, output } => {
            ResponseInputItem::FunctionCallOutput {
                call_id,
                output: compact_payload(output, tool_name).await,
            }
        }
        ResponseInputItem::CustomToolCallOutput {
            call_id,
            name,
            output,
        } => ResponseInputItem::CustomToolCallOutput {
            call_id,
            name,
            output: compact_payload(output, tool_name).await,
        },
        other => other,
    }
}

async fn compact_payload(
    mut output: FunctionCallOutputPayload,
    tool_name: &str,
) -> FunctionCallOutputPayload {
    match &mut output.body {
        FunctionCallOutputBody::Text(text) => {
            let compacted = compact_text(std::mem::take(text), tool_name).await;
            output.body = FunctionCallOutputBody::Text(compacted);
        }
        FunctionCallOutputBody::ContentItems(items) => {
            for item in items.iter_mut() {
                if let FunctionCallOutputContentItem::InputText { text } = item {
                    *text = compact_text(std::mem::take(text), tool_name).await;
                }
            }
        }
    }
    output
}

async fn compact_text(content: String, tool_name: &str) -> String {
    let enabled = wormhole_tokenjuice::current_options().router_enabled;
    if !enabled {
        return content;
    }
    let original_len = content.len();
    let compacted = compact_output_with_policy(
        content,
        tool_name,
        /*enabled*/ true,
        AgentTokenjuiceCompression::Full,
    )
    .await;
    if compacted.len() != original_len {
        debug!(
            tool = %tool_name,
            original_bytes = original_len,
            compacted_bytes = compacted.len(),
            "[tokenjuice] compacted tool output"
        );
    }
    compacted
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use wormhole_tokenjuice::TokenjuiceConfig;
    use wormhole_tokenjuice::install_from_config;

    #[tokio::test]
    async fn recovery_tool_is_not_compacted() {
        let dir = tempdir().unwrap();
        install_from_config(&TokenjuiceConfig::default(), dir.path(), None, 5.0);
        let original = "x".repeat(4096);
        let item = ResponseInputItem::FunctionCallOutput {
            call_id: "c1".into(),
            output: FunctionCallOutputPayload::from_text(original.clone()),
        };
        let out = compact_response_input_item(item, "tinyjuice_retrieve").await;
        match out {
            ResponseInputItem::FunctionCallOutput { output, .. } => {
                assert_eq!(output.text_content(), Some(original.as_str()));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn small_output_passes_through() {
        let dir = tempdir().unwrap();
        let mut cfg = TokenjuiceConfig::default();
        cfg.min_bytes_to_compress = 2048;
        install_from_config(&cfg, dir.path(), None, 5.0);
        let original = "hello";
        let item = ResponseInputItem::FunctionCallOutput {
            call_id: "c1".into(),
            output: FunctionCallOutputPayload::from_text(original.into()),
        };
        let out = compact_response_input_item(item, "shell").await;
        match out {
            ResponseInputItem::FunctionCallOutput { output, .. } => {
                assert_eq!(output.text_content(), Some(original));
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
