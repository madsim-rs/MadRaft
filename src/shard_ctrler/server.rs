use super::msg::*;
use crate::kvraft::server::{Server, State};
use serde::{Deserialize, Serialize};

pub type ShardCtrler = Server<ShardInfo>;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ShardInfo {
    // Your data here.
}

impl State for ShardInfo {
    type Command = Op;
    type Output = Option<Config>;

    fn apply(&mut self, cmd: Self::Command) -> Self::Output {
        todo!("apply command");
    }
}
