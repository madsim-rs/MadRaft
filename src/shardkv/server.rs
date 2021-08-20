use super::msg::*;
use crate::kvraft::server::{Server, State};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type ShardKvServer = Server<ShardKv>;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ShardKv {
    kv: HashMap<String, String>,
    // A circular queue with max capacity 50
    ids: Vec<u64>,
}

impl State for ShardKv {
    type Command = Op;
    type Output = Reply;

    fn apply(&mut self, id: u64, cmd: Self::Command) -> Self::Output {
        let unique = !self.ids.contains(&id);
        if self.ids.len() > 50 {
            self.ids.remove(0);
        }
        self.ids.push(id);
        match cmd {
            Op::Put { key, value } if unique => {
                self.kv.insert(key, value);
                Reply::Ok
            }
            Op::Append { key, value } if unique => {
                self.kv.entry(key).or_default().push_str(&value);
                Reply::Ok
            }
            Op::Get { key } => Reply::Get {
                value: self.kv.get(&key).cloned().unwrap_or_default(),
            },
            _ => Reply::Ok,
        }
    }
}
