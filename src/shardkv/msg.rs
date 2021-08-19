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

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Error {
    #[error("not leader")]
    NotLeader(usize),
    #[error("wrong group")]
    WrongGroup,
    #[error("timeout")]
    Timeout,
    #[error("failed to reach consensus")]
    Failed,
}
