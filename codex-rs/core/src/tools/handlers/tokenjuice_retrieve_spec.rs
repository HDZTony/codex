use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolSpec;
use serde_json::json;
use std::collections::BTreeMap;

pub fn create_tokenjuice_retrieve_tool(name: &str) -> ToolSpec {
    let range_properties = BTreeMap::from([
        (
            "start".to_string(),
            JsonSchema::number(Some("Inclusive start offset.".to_string())),
        ),
        (
            "end".to_string(),
            JsonSchema::number(Some("Exclusive end offset.".to_string())),
        ),
        (
            "unit".to_string(),
            JsonSchema::string_enum(
                vec![json!("bytes"), json!("lines")],
                Some("Offset unit (default bytes).".to_string()),
            ),
        ),
    ]);

    let properties = BTreeMap::from([
        (
            "token".to_string(),
            JsonSchema::string(Some(
                "CCR token from a TokenJuice footer (⟦tj:<hash>⟧).".to_string(),
            )),
        ),
        (
            "hash".to_string(),
            JsonSchema::string(Some(
                "Legacy alias for `token`.".to_string(),
            )),
        ),
        (
            "range".to_string(),
            JsonSchema::object(
                range_properties,
                None,
                Some(false.into()),
            ),
        ),
    ]);

    ToolSpec::Function(ResponsesApiTool {
        name: name.to_string(),
        description: r#"Retrieve a full original tool output previously offloaded by TokenJuice.
Use the token from a compacted footer (⟦tj:<hash>⟧ / tinyjuice_retrieve).
Optional range returns a byte or line slice.
"#
        .to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(properties, None, Some(false.into())),
        output_schema: None,
    })
}
