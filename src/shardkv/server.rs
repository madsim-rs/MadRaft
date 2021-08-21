use super::{key2shard, msg::*};
use crate::kvraft::{
    client::ClerkCore,
    server::{Server, State},
};
use crate::shard_ctrler::{client::Clerk as CtrlerClerk, msg::Config, N_SHARDS};
use madsim::{task, time::*};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::Arc,
};

pub struct ShardKvServer {
    inner: Arc<Server<ShardKv>>,
    _bg_task: task::Task<()>,
}

impl ShardKvServer {
    pub async fn new(
        ctrl_ck: CtrlerClerk,
        servers: Vec<SocketAddr>,
        gid: u64,
        me: usize,
        max_raft_state: Option<usize>,
    ) -> Arc<Self> {
        let self_ck = ClerkCore::<Op, Reply>::new(servers.clone());
        let state0 = ShardKv {
            gid,
            ..Default::default()
        };
        let inner = Server::new_with_state(servers, me, max_raft_state, state0).await;
        let state = inner.state().clone();
        // background task to periodically poll new configurations
        let _bg_task = task::spawn(async move {
            loop {
                let num = state.lock().unwrap().cfg.num;
                let cfg = ctrl_ck.query_at(num + 1).await;
                let need = {
                    let state = state.lock().unwrap();
                    cfg.num == state.cfg.num + 1 && state.next_cfg.is_none()
                };
                if need {
                    self_ck.call(Op::CfgChange { cfg }).await;
                }
                sleep(Duration::from_millis(100)).await;
            }
        });
        Arc::new(ShardKvServer { inner, _bg_task })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ShardKv {
    /// My GID.
    gid: u64,
    /// Current config.
    cfg: Config,
    /// If this is Some, the server is undergoing config change.
    next_cfg: Option<Config>,
    /// Shards.
    shards: HashMap<usize, Shard>,
    /// Recent command IDs. To prevent duplicate.
    ids: VecDeque<u64>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Shard {
    kv: HashMap<String, String>,
}

impl State for ShardKv {
    type Command = Op;
    type Output = Reply;

    fn apply(&mut self, id: u64, cmd: Self::Command) -> Self::Output {
        let unique = !self.ids.contains(&id);
        if self.ids.len() > 50 {
            self.ids.pop_front();
        }
        self.ids.push_back(id);

        match cmd {
            Op::Put { key, value } if unique => {
                if let Some(shard) = self.try_serve(&key) {
                    shard.kv.insert(key, value);
                } else {
                    // TODO: return Moving?
                    return Reply::WrongGroup;
                }
            }
            Op::Append { key, value } if unique => {
                if let Some(shard) = self.try_serve(&key) {
                    shard.kv.entry(key).or_default().push_str(&value);
                } else {
                    // TODO: return Moving?
                    return Reply::WrongGroup;
                }
            }
            Op::Get { key } => {
                if let Some(shard) = self.try_serve(&key) {
                    return Reply::Get {
                        value: shard.kv.get(&key).cloned(),
                    };
                } else {
                    // TODO: return Moving?
                    return Reply::WrongGroup;
                }
            }
            Op::CfgChange { cfg } if unique => {
                if !(cfg.num == self.cfg.num + 1 && self.next_cfg.is_none()) {
                    return Reply::WrongCfg;
                }
                if cfg.num == 1 {
                    for shard in 0..N_SHARDS {
                        if cfg.shards[shard] == self.gid {
                            self.shards.insert(shard, Shard::default());
                        }
                    }
                    self.cfg = cfg;
                    return Reply::Ok;
                }
                debug!("begin config change: {}->{}", self.cfg.num, cfg.num);
                for shard in (0..N_SHARDS)
                    .filter(|&s| cfg.shards[s] != self.gid && self.cfg.shards[s] == self.gid)
                {
                    let dst_gid = cfg.shards[shard];
                    let dst_ck = ClerkCore::<Op, Reply>::new(cfg.groups[&dst_gid].clone());
                    let put = Op::PutShard {
                        cfg_num: self.cfg.num,
                        shard,
                        kv: self.shards[&shard].kv.clone(),
                    };
                    let self_ck = ClerkCore::<Op, Reply>::new(self.cfg.groups[&self.gid].clone());
                    let del = Op::DelShard {
                        cfg_num: self.cfg.num,
                        shard,
                    };
                    task::spawn(async move {
                        loop {
                            match dst_ck.call(put.clone()).await {
                                // wait for the group to enter the migration state
                                Reply::WrongCfg => sleep(Duration::from_millis(100)).await,
                                Reply::Ok => break,
                                e => panic!("unexpected reply: {:?}", e),
                            }
                        }
                        self_ck.call(del).await;
                    })
                    .detach();
                }
                self.next_cfg = Some(cfg);
                self.try_complete_config_change();
            }
            Op::PutShard { cfg_num, shard, kv } if unique => {
                if self.cfg.num != cfg_num || self.next_cfg.is_none() {
                    return Reply::WrongCfg;
                }
                if self.shards.contains_key(&shard) {
                    return Reply::Ok;
                }
                self.shards.insert(shard, Shard { kv });
                self.try_complete_config_change();
            }
            Op::DelShard { cfg_num, shard } if unique => {
                if self.cfg.num != cfg_num || self.next_cfg.is_none() {
                    return Reply::WrongCfg;
                }
                self.shards.remove(&shard);
                self.try_complete_config_change();
            }
            _ => {}
        }
        Reply::Ok
    }
}

impl ShardKv {
    fn try_serve(&mut self, key: &str) -> Option<&mut Shard> {
        let shard = key2shard(key);
        if self.can_serve(shard) {
            Some(self.shards.get_mut(&shard).unwrap())
        } else {
            None
        }
    }

    fn can_serve(&self, shard: usize) -> bool {
        if let Some(next_cfg) = &self.next_cfg {
            // challenge2
            let unaffected = self.cfg.shards[shard] == self.gid;
            let received = self.shards.contains_key(&shard);
            next_cfg.shards[shard] == self.gid && (unaffected || received)
        } else {
            self.cfg.shards[shard] == self.gid
        }
    }

    fn try_complete_config_change(&mut self) {
        let next_cfg = self.next_cfg.as_ref().unwrap();
        for s in 0..N_SHARDS {
            if (next_cfg.shards[s] == self.gid) ^ self.shards.contains_key(&s) {
                return;
            }
        }
        debug!("end config change: {}->{}", self.cfg.num, next_cfg.num);
        self.cfg = self.next_cfg.take().unwrap();
    }
}
