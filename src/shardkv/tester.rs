use super::{client::Clerk, server::ShardKvServer};
use crate::shard_ctrler::{client::Clerk as CtrlerClerk, server::ShardCtrler, N_SHARDS};
use ::rand::distributions::Alphanumeric;
use madsim::{
    rand::{self, Rng},
    time::*,
    Handle,
};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, Mutex},
};

pub struct Tester {
    handle: Handle,
    n: usize,

    ctrler_addrs: Vec<SocketAddr>,
    _ctrlers: Vec<Arc<ShardCtrler>>,
    ctrler_ck: CtrlerClerk,

    groups: Vec<Group>,

    max_raft_state: Option<usize>,

    // begin()/end() statistics
    t0: Instant,
}

struct Group {
    gid: u64,
    addrs: Vec<SocketAddr>,
    servers: Mutex<Vec<Option<Arc<ShardKvServer>>>>,
}

impl Tester {
    pub async fn new(n: usize, unreliable: bool, max_raft_state: Option<usize>) -> Tester {
        let handle = Handle::current();
        if unreliable {
            handle.net.update_config(|cfg| {
                cfg.packet_loss_rate = 0.1;
                cfg.send_latency = Duration::from_millis(1)..Duration::from_millis(27);
            });
        }

        let n_ctrler = 3;
        let ctrler_addrs = (0..n_ctrler)
            .map(|i| SocketAddr::from(([0, 0, 1, i as _], 0)))
            .collect::<Vec<_>>();
        let mut ctrlers = vec![];
        for i in 0..n_ctrler {
            let handle = handle.local_handle(ctrler_addrs[i]);
            let ctrler = handle
                .spawn(ShardCtrler::new(ctrler_addrs.clone(), i, max_raft_state))
                .await;
            ctrlers.push(ctrler);
        }
        let ctrler_ck = CtrlerClerk::new(ctrler_addrs.clone());

        let n_groups: usize = 3;
        let groups = (0..n_groups)
            .map(|i| Group {
                gid: i as u64 + 100,
                addrs: (0..n)
                    .map(|j| SocketAddr::from(([0, 1, i as _, j as _], 0)))
                    .collect(),
                servers: Mutex::new(vec![None; n]),
            })
            .collect();

        let tester = Tester {
            handle,
            n,
            ctrler_addrs,
            _ctrlers: ctrlers,
            ctrler_ck,
            groups,
            max_raft_state,
            t0: Instant::now(),
        };
        for g in 0..n_groups {
            for i in 0..n {
                tester.start_server(g, i).await;
            }
        }
        tester
    }

    /// check that no server's log is too big.
    pub fn check_logs(&self) {
        for group in self.groups.iter() {
            for &addr in group.addrs.iter() {
                let state_size = self.handle.fs.get_file_size(addr, "state").unwrap_or(0);
                let snap_size = self.handle.fs.get_file_size(addr, "snapshot").unwrap_or(0);
                if let Some(limit) = self.max_raft_state {
                    assert!(
                        state_size as usize <= 8 * limit,
                        "raft state size {} exceed limit {}",
                        state_size,
                        limit
                    );
                } else {
                    assert_eq!(
                        snap_size, 0,
                        "max_raft_state is None, but snapshot is non-empty!"
                    );
                }
            }
        }
    }

    pub fn total_size(&self) -> u64 {
        let mut size = 0;
        for group in self.groups.iter() {
            for &addr in group.addrs.iter() {
                let state_size = self.handle.fs.get_file_size(addr, "state").unwrap();
                let snap_size = self.handle.fs.get_file_size(addr, "snapshot").unwrap();
                size += state_size + snap_size;
            }
        }
        size
    }

    fn rpc_total(&self) -> u64 {
        self.handle.net.stat().msg_count / 2
    }

    // Create a clerk with clerk specific server names.
    // Give it connections to all of the servers
    pub fn make_client(&self) -> Clerk {
        Clerk::new(self.ctrler_addrs.clone())
    }

    /// Start i'th server of group.
    pub async fn start_server(&self, group: usize, i: usize) {
        debug!("start_server({}, {})", group, i);
        let group = &self.groups[group];
        let addrs = group.addrs.clone();
        let handle = self.handle.local_handle(group.addrs[i]);
        let ctrl_ck = CtrlerClerk::new(self.ctrler_addrs.clone());
        let kv = handle
            .spawn(ShardKvServer::new(
                ctrl_ck,
                addrs,
                group.gid,
                i,
                self.max_raft_state,
            ))
            .await;
        group.servers.lock().unwrap()[i] = Some(kv);
    }

    /// Shutdown i'th server of group.
    pub fn shutdown_server(&self, group: usize, i: usize) {
        debug!("shutdown_server({}, {})", group, i);
        let group = &self.groups[group];
        self.handle.kill(group.addrs[i]);
        group.servers.lock().unwrap()[i] = None;
    }

    pub async fn start_group(&self, group: usize) {
        for i in 0..self.n {
            self.start_server(group, i).await;
        }
    }

    pub fn shutdown_group(&self, group: usize) {
        for i in 0..self.n {
            self.shutdown_server(group, i);
        }
    }

    // tell the shardctrler that a group is joining.
    pub async fn join(&self, group: usize) {
        self.joins(&[group]).await;
    }

    pub async fn joins(&self, groups: &[usize]) {
        debug!("join({:?})", groups);
        let mut m = HashMap::new();
        for &g in groups {
            let gid = self.groups[g].gid;
            let names = self.groups[g].addrs.clone();
            m.insert(gid, names);
        }
        self.ctrler_ck.join(m).await;
    }

    // tell the shardctrler that a group is leaving.
    pub async fn leave(&self, group: usize) {
        self.leaves(&[group]).await;
    }

    pub async fn leaves(&self, groups: &[usize]) {
        debug!("leave({:?})", groups);
        let gids: Vec<u64> = groups.iter().map(|&g| self.groups[g].gid).collect();
        self.ctrler_ck.leave(&gids).await;
    }

    /// QUERY to find shards now owned by group
    pub async fn query_shards_of(&self, group: usize) -> HashSet<usize> {
        let c = self.ctrler_ck.query().await;
        let gid = self.groups[group].gid;
        (0..N_SHARDS).filter(|&i| c.shards[i] == gid).collect()
    }

    /// End a Test -- the fact that we got here means there
    /// was no failure.
    /// print the Passed message,
    /// and some performance numbers.
    pub fn end(&self) {
        self.check_timeout();

        // real time
        let t = self.t0.elapsed();
        // number of Raft peers
        let npeers = self.n;
        // number of RPC sends
        let nrpc = self.rpc_total();

        info!("  ... Passed --");
        info!("  {:?}  {} {}", t, npeers, nrpc);
    }

    fn check_timeout(&self) {
        // enforce a two minute real-time limit on each test
        if self.t0.elapsed() > Duration::from_secs(120) {
            panic!("test took longer than 120 seconds");
        }
    }
}

impl Clerk {
    pub async fn put_kvs(&self, kvs: &[(String, String)]) {
        for (k, v) in kvs {
            self.put(k.clone(), v.clone()).await;
        }
    }

    pub async fn check(&self, k: String, v: String) {
        let v0 = self.get(k.clone()).await;
        assert_eq!(v0, v, "check failed: key={:?}", k);
    }

    pub async fn check_kvs(&self, kvs: &[(String, String)]) {
        for (k, v) in kvs {
            let v0 = self.get(k.clone()).await;
            assert_eq!(&v0, v, "check failed: key={:?}", k);
        }
    }

    pub async fn check_append_kvs(&self, kvs: &mut Vec<(String, String)>, len: usize) {
        for (k, v) in kvs.iter_mut() {
            let v0 = self.get(k.clone()).await;
            assert_eq!(&v0, v, "check failed: key={:?}", k);
            let s = rand_string(len);
            *v += &s;
            self.append(k.clone(), s).await;
        }
    }
}

pub fn rand_string(len: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
