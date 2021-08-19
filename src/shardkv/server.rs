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

pub struct ShardKv {
    rf: raft::RaftHandle,
    me: usize,
    // { index -> (id, sender) }
    pending_rpcs: Arc<Mutex<HashMap<u64, (u64, oneshot::Sender<String>)>>>,
    _bg_task: task::Task<()>,
}

impl fmt::Debug for ShardKv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ShardKv({})", self.me)
    }
}

impl ShardKv {
    pub async fn new(
        servers: Vec<SocketAddr>,
        me: usize,
        max_raft_state: Option<usize>,
    ) -> Arc<Self> {
        // You may need initialization code here.
        let (rf, mut apply_ch) = raft::RaftHandle::new(servers, me).await;

        let pending_rpcs = Arc::new(Mutex::new(
            HashMap::<u64, (u64, oneshot::Sender<String>)>::new(),
        ));
        let pending_rpcs0 = pending_rpcs.clone();
        let rf0 = rf.clone();
        let _bg_task = task::spawn_local(async move {
            let mut state = Kv::default();
            let mut state_index;
            while let Some(msg) = apply_ch.next().await {
                match msg {
                    raft::ApplyMsg::Snapshot { index, data, .. } => {
                        state = bincode::deserialize(&data).unwrap();
                        state_index = index;
                    }
                    raft::ApplyMsg::Command { index, data } => {
                        let (id, cmd): (u64, Op) = bincode::deserialize(&data).unwrap();
                        let ret = state.apply(id, cmd);
                        state_index = index;

                        // send result to RPC
                        let mut pending_rpcs = pending_rpcs0.lock().unwrap();
                        if let Some((id0, sender)) = pending_rpcs.remove(&index) {
                            if id == id0 {
                                // message match, success
                                let _ = sender.send(ret);
                            }
                            // otherwise drop the sender
                        }
                    }
                }
                // snapshot if needed
                if let Some(size) = max_raft_state {
                    if fs::metadata("state").await.map(|m| m.len()).unwrap_or(0) >= size as u64 {
                        let data = bincode::serialize(&state).unwrap();
                        rf0.snapshot(state_index, &data).await.unwrap();
                    }
                }
            }
        });

        let this = Arc::new(ShardKv {
            rf,
            me,
            pending_rpcs,
            _bg_task,
        });
        this.start_rpc_server();
        this
    }

    fn start_rpc_server(self: &Arc<Self>) {
        let net = net::NetLocalHandle::current();

        let this = self.clone();
        net.add_rpc_handler(move |(id, cmd): (u64, Op)| {
            let this = this.clone();
            async move { this.apply(id, cmd).await }
        });
    }

    fn register_rpc(&self, index: u64, id: u64) -> oneshot::Receiver<String> {
        let (sender, recver) = oneshot::channel();
        self.pending_rpcs
            .lock()
            .unwrap()
            .insert(index, (id, sender));
        recver
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.rf.term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.rf.is_leader()
    }

    async fn apply(&self, id: u64, cmd: Op) -> Result<String, Error> {
        debug!("{:?} start: id={} {:?}", self, id, cmd);
        let index = match self
            .rf
            .start(&bincode::serialize(&(id, cmd)).unwrap())
            .await
        {
            Ok(s) => s.index,
            Err(raft::Error::NotLeader(l)) => return Err(Error::NotLeader(l)),
            _ => unreachable!(),
        };
        let recver = self.register_rpc(index, id);
        let output = timeout(Duration::from_millis(500), recver)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::Failed)?;
        Ok(output)
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Kv {
    kv: HashMap<String, String>,
    // A circular queue with max capacity 50
    ids: Vec<u64>,
}

impl Kv {
    fn apply(&mut self, id: u64, cmd: Op) -> String {
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
