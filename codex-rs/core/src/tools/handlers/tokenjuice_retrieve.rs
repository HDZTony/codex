//! TokenJuice CCR retrieve tool (`tinyjuice_retrieve` + legacy aliases).

use crate::function_tool::FunctionCallError;
use crate::tools::context::FunctionToolOutput;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolPayload;
use crate::tools::context::boxed_tool_output;
use crate::tools::handlers::parse_arguments;
use crate::tools::handlers::tokenjuice_retrieve_spec::create_tokenjuice_retrieve_tool;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::ToolExecutor;
use crate::tools::registry::ToolExposure;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct RetrieveArgs {
    /// Canonical CCR token (SHA-256 hex).
    #[serde(default)]
    token: Option<String>,
    /// Legacy alias for `token`.
    #[serde(default)]
    hash: Option<String>,
    #[serde(default)]
    range: Option<RetrieveRange>,
}

#[derive(Debug, Deserialize)]
struct RetrieveRange {
    start: usize,
    end: usize,
    #[serde(default)]
    unit: Option<String>,
}

/// Canonical retrieve tool advertised in CCR footers.
pub struct TokenjuiceRetrieveHandler {
    name: &'static str,
    exposure: ToolExposure,
}

impl TokenjuiceRetrieveHandler {
    pub fn canonical() -> Self {
        Self {
            name: wormhole_tokenjuice::RETRIEVE_TOOL_NAME,
            exposure: ToolExposure::Direct,
        }
    }

    pub fn legacy_tokenjuice() -> Self {
        Self {
            name: "tokenjuice_retrieve",
            exposure: ToolExposure::Hidden,
        }
    }

    pub fn legacy_retrieve_tool_output() -> Self {
        Self {
            name: wormhole_tokenjuice::LEGACY_RETRIEVE_TOOL_NAME,
            exposure: ToolExposure::Hidden,
        }
    }
}

#[async_trait::async_trait]
impl ToolExecutor<ToolInvocation> for TokenjuiceRetrieveHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain(self.name)
    }

    fn spec(&self) -> ToolSpec {
        create_tokenjuice_retrieve_tool(self.name)
    }

    fn exposure(&self) -> ToolExposure {
        self.exposure
    }

    async fn handle(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn crate::tools::context::ToolOutput>, FunctionCallError> {
        crate::tokenjuice_compact::ensure_tokenjuice_installed();

        let ToolInvocation { payload, .. } = invocation;
        let arguments = match payload {
            ToolPayload::Function { arguments } => arguments,
            _ => {
                return Err(FunctionCallError::RespondToModel(
                    "tokenjuice_retrieve received unsupported payload".to_string(),
                ));
            }
        };

        let args: RetrieveArgs = parse_arguments(&arguments)?;
        let token = args
            .token
            .or(args.hash)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                FunctionCallError::RespondToModel(
                    "tokenjuice_retrieve requires `token` (or legacy `hash`)".to_string(),
                )
            })?;

        let (start, end, unit) = match args.range {
            Some(r) => (Some(r.start), Some(r.end), r.unit),
            None => (None, None, None),
        };

        let result = wormhole_tokenjuice::retrieve(&token, start, end, unit.as_deref());

        let content = if result.found {
            result.content.unwrap_or_default()
        } else {
            format!("CCR original not found for token {token}")
        };

        Ok(boxed_tool_output(FunctionToolOutput::from_text(
            content,
            Some(result.found),
        )))
    }
}

impl CoreToolRuntime for TokenjuiceRetrieveHandler {}
