use super::msg::*;
use crate::kvraft::server::{Server, State};
use crate::shard_ctrler::client::Clerk as CtrlerClerk;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};

pub struct ShardKvServer {
    inner: Arc<Server<ShardKv>>,
}

impl ShardKvServer {
    pub async fn new(
        ctrl_ck: CtrlerClerk,
        servers: Vec<SocketAddr>,
        gid: u64,
        me: usize,
        max_raft_state: Option<usize>,
    ) -> Arc<Self> {
        todo!("construct ShardKv");
        let inner = Server::new(servers, me, max_raft_state).await;
        Arc::new(ShardKvServer { inner })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ShardKv {
    // Your data here.
}

impl State for ShardKv {
    type Command = Op;
    type Output = Reply;

    fn apply(&mut self, cmd: Self::Command) -> Self::Output {
        todo!("apply command");
    }
}
