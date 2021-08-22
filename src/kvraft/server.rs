use super::msg::*;
use crate::raft;
use madsim::net;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Debug},
    marker::PhantomData,
    net::SocketAddr,
    sync::Arc,
};

pub trait State: net::Message + Default {
    type Command: net::Message + Clone;
    type Output: net::Message;
    fn apply(&mut self, cmd: Self::Command) -> Self::Output;
}

pub struct Server<S: State> {
    rf: raft::RaftHandle,
    me: usize,
    _marker: PhantomData<S>,
}

impl<S: State> fmt::Debug for Server<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Server({})", self.me)
    }
}

impl<S: State> Server<S> {
    pub async fn new(
        servers: Vec<SocketAddr>,
        me: usize,
        max_raft_state: Option<usize>,
    ) -> Arc<Self> {
        // You may need initialization code here.
        let (rf, apply_ch) = raft::RaftHandle::new(servers, me).await;

        let this = Arc::new(Server {
            rf,
            me,
            _marker: PhantomData,
        });
        this.start_rpc_server();
        this
    }

    fn start_rpc_server(self: &Arc<Self>) {
        let net = net::NetLocalHandle::current();

        let this = self.clone();
        net.add_rpc_handler(move |cmd: S::Command| {
            let this = this.clone();
            async move { this.apply(cmd).await }
        });
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.rf.term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.rf.is_leader()
    }

    async fn apply(&self, cmd: S::Command) -> Result<S::Output, Error> {
        todo!("apply command");
    }
}

pub type KvServer = Server<Kv>;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Kv {
    // Your data here.
}

impl State for Kv {
    type Command = Op;
    type Output = String;

    fn apply(&mut self, cmd: Self::Command) -> Self::Output {
        todo!("apply command");
    }
}
