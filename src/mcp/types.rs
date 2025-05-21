use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize, Debug)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: Option<Value>,
    pub method: String,
    pub params: Option<Value>,
}

pub const INVALID_PARAMS: i32 = -32602;
pub const METHOD_NOT_FOUND: i32 = -32601;
pub const INTERNAL_ERROR: i32 = -32603;
