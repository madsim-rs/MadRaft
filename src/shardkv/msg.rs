use crate::shard_ctrler::msg::{Config, ConfigId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    // issued by client
    Get {
        key: String,
    },
    Put {
        key: String,
        value: String,
    },
    Append {
        key: String,
        value: String,
    },

    // issued by self or other servers
    CfgChange {
        cfg: Config,
    },
    PutShard {
        cfg_num: ConfigId,
        shard: usize,
        kv: HashMap<String, String>,
    },
    DelShard {
        cfg_num: ConfigId,
        shard: usize,
    },
}

impl Op {
    pub fn key(&self) -> &str {
        match self {
            Op::Get { key } => key,
            Op::Put { key, .. } => key,
            Op::Append { key, .. } => key,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Reply {
    Get {
        value: Option<String>,
    },
    Ok,
    /// Data is moving during configuration change.
    Moving,
    WrongCfg,
    WrongGroup,
}
