use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    Get { key: String },
    Put { key: String, value: String },
    Append { key: String, value: String },
}

impl Op {
    pub fn key(&self) -> &str {
        match self {
            Op::Get { key } => key,
            Op::Put { key, .. } => key,
            Op::Append { key, .. } => key,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Reply {
    Get { value: String },
    Ok,
    WrongGroup,
}
