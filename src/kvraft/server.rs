use super::msg::*;
use crate::raft;
use futures::{channel::oneshot, StreamExt};
use madsim::{
    fs, net, task,
    time::{timeout, Duration},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{self, Debug},
    net::SocketAddr,
    sync::{Arc, Mutex},
};

pub trait State: net::Message {
    type Command: net::Message + Clone;
    type Output: net::Message;
    fn apply(&mut self, id: u64, cmd: Self::Command) -> Self::Output;
}

pub struct Server<S: State> {
    rf: raft::RaftHandle,
    me: usize,
    rpcs: Arc<Rpcs<S::Output>>,
    state: Arc<Mutex<S>>,
    _bg_task: task::Task<()>,
}

impl<S: State> fmt::Debug for Server<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Server({})", self.me)
    }
}

impl<S: State + Default> Server<S> {
    pub async fn new(
        servers: Vec<SocketAddr>,
        me: usize,
        max_raft_state: Option<usize>,
    ) -> Arc<Self> {
        Self::new_with_state(servers, me, max_raft_state, S::default()).await
    }
}

impl<S: State> Server<S> {
    pub async fn new_with_state(
        servers: Vec<SocketAddr>,
        me: usize,
        max_raft_state: Option<usize>,
        state0: S,
    ) -> Arc<Self> {
        // You may need initialization code here.
        let (rf, mut apply_ch) = raft::RaftHandle::new(servers, me).await;

        let rpcs = Arc::new(Rpcs::default());
        let state = Arc::new(Mutex::new(state0));

        let rpcs0 = rpcs.clone();
        let rf0 = rf.clone();
        let state0 = state.clone();
        let _bg_task = task::spawn_local(async move {
            while let Some(msg) = apply_ch.next().await {
                let state_index;
                match msg {
                    raft::ApplyMsg::Snapshot { index, data, .. } => {
                        debug!("apply snapshot at index {}", index);
                        *state0.lock().unwrap() = bincode::deserialize(&data).unwrap();
                        state_index = index;
                    }
                    raft::ApplyMsg::Command { index, data } => {
                        let (id, cmd): (u64, S::Command) = bincode::deserialize(&data).unwrap();
                        let ret = state0.lock().unwrap().apply(id, cmd.clone());
                        debug!("apply [{:04x}] {:?} => {:?}", id as u16, cmd, ret);
                        state_index = index;
                        rpcs0.complete(index, id, ret);
                    }
                }
                // snapshot if needed
                if let Some(size) = max_raft_state {
                    if fs::metadata("state").await.map(|m| m.len()).unwrap_or(0) >= size as u64 {
                        let data = bincode::serialize(&*state0.lock().unwrap()).unwrap();
                        rf0.snapshot(state_index, &data).await.unwrap();
                    }
                }
            }
        });

        let this = Arc::new(Server {
            rf,
            me,
            rpcs,
            state,
            _bg_task,
        });
        this.start_rpc_server();
        this
    }

    fn start_rpc_server(self: &Arc<Self>) {
        let net = net::NetLocalHandle::current();

        let this = self.clone();
        net.add_rpc_handler(move |(id, cmd): (u64, S::Command)| {
            let this = this.clone();
            async move { this.apply(id, cmd).await }
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

    pub fn state(&self) -> &Arc<Mutex<S>> {
        &self.state
    }

    async fn apply(&self, id: u64, cmd: S::Command) -> Result<S::Output, Error> {
        let index = match self
            .rf
            .start(&bincode::serialize(&(id, cmd)).unwrap())
            .await
        {
            Ok(s) => s.index,
            Err(raft::Error::NotLeader(hint)) => return Err(Error::NotLeader { hint }),
            _ => unreachable!(),
        };
        let recver = self.rpcs.register(index, id);
        let output = timeout(Duration::from_millis(500), recver)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::Failed)?;
        Ok(output)
    }
}

/// Pending RPCs register center.
struct Rpcs<T> {
    // { index -> (id, sender) }
    rpcs: Mutex<HashMap<u64, (u64, oneshot::Sender<T>)>>,
}

impl<T> Default for Rpcs<T> {
    fn default() -> Self {
        Self {
            rpcs: Default::default(),
        }
    }
}

impl<T> Rpcs<T> {
    fn register(&self, index: u64, id: u64) -> oneshot::Receiver<T> {
        let (sender, recver) = oneshot::channel();
        self.rpcs.lock().unwrap().insert(index, (id, sender));
        recver
    }

    fn complete(&self, index: u64, id: u64, value: T) {
        let mut rpcs = self.rpcs.lock().unwrap();
        if let Some((id0, sender)) = rpcs.remove(&index) {
            if id == id0 {
                // message match, success
                let _ = sender.send(value);
            }
            // otherwise drop the sender
        }
    }
}

pub type KvServer = Server<Kv>;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Kv {
    kv: HashMap<String, String>,
    // A circular queue with max capacity 50
    ids: Vec<u64>,
}

impl State for Kv {
    type Command = Op;
    type Output = String;

    fn apply(&mut self, id: u64, cmd: Self::Command) -> Self::Output {
        let unique = !self.ids.contains(&id);
        if self.ids.len() > 50 {
            self.ids.remove(0);
        }
        self.ids.push(id);
        match cmd {
            Op::Put { key, value } if unique => {
                self.kv.insert(key, value);
            }
            Op::Append { key, value } if unique => {
                self.kv.entry(key).or_default().push_str(&value);
            }
            Op::Get { key } => return self.kv.get(&key).cloned().unwrap_or_default(),
            _ => {}
        }
        "".into()
    }
}
