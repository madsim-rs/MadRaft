use madsim::{time::*, Handle};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use super::{client::Clerk, server::ShardCtrler};

pub struct Tester {
    handle: Handle,
    n: usize,
    addrs: Vec<SocketAddr>,
    servers: Mutex<Vec<Option<Arc<ShardCtrler>>>>,

    // begin()/end() statistics
    t0: Instant,
}

impl Tester {
    pub async fn new(n: usize, unreliable: bool) -> Tester {
        let handle = Handle::current();
        if unreliable {
            handle.net.update_config(|cfg| {
                cfg.packet_loss_rate = 0.1;
                cfg.send_latency = Duration::from_millis(1)..Duration::from_millis(27);
            });
        }
        let mut servers = vec![];
        servers.resize_with(n, || None);
        let tester = Tester {
            handle,
            n,
            addrs: (0..n)
                .map(|i| SocketAddr::from(([0, 0, 1, i as _], 0)))
                .collect::<Vec<_>>(),
            servers: Mutex::new(servers),
            t0: Instant::now(),
        };
        // create a full set of KV servers.
        for i in 0..n {
            tester.start_server(i).await;
        }
        tester
    }

    fn rpc_total(&self) -> u64 {
        self.handle.net.stat().msg_count / 2
    }

    fn check_timeout(&self) {
        // enforce a two minute real-time limit on each test
        if self.t0.elapsed() > Duration::from_secs(120) {
            panic!("test took longer than 120 seconds");
        }
    }

    // Create a clerk with clerk specific server names.
    // Give it connections to all of the servers, but for
    // now enable only connections to servers in to[].
    pub fn make_client(&self) -> Clerk {
        Clerk::new(self.addrs.clone())
    }

    /// Shutdown a server.
    pub fn shutdown_server(&self, i: usize) {
        debug!("shutdown_server({})", i);
        self.handle.kill(self.addrs[i]);
        self.servers.lock().unwrap()[i] = None;
    }

    /// Start a server.
    /// If restart servers, first call shutdown_server
    pub async fn start_server(&self, i: usize) {
        debug!("start_server({})", i);
        let addrs = self.addrs.clone();
        let handle = self.handle.local_handle(self.addrs[i]);
        let kv = handle.spawn(ShardCtrler::new(addrs, i, None)).await;
        self.servers.lock().unwrap()[i] = Some(kv);
    }

    pub fn leader(&self) -> Option<usize> {
        let servers = self.servers.lock().unwrap();
        for (i, kv) in servers.iter().enumerate() {
            if let Some(kv) = kv {
                if kv.is_leader() {
                    return Some(i);
                }
            }
        }
        None
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
}

impl Clerk {
    pub async fn check(&self, groups: &[u64]) {
        debug!("check: {:?}", groups);
        let c = self.query().await;
        assert_eq!(c.groups.len(), groups.len());
        // are the groups as expected?
        for gid in groups {
            assert!(c.groups.contains_key(gid), "missing group {}", gid);
        }
        // any un-allocated shards?
        if groups.is_empty() {
            for (shard, gid) in c.shards.iter().enumerate() {
                assert!(
                    *gid == 0 || c.groups.contains_key(gid),
                    "shard {} -> invalid group {}",
                    shard,
                    gid
                );
            }
        }
        // more or less balanced sharding?
        let mut counts = HashMap::<u64, usize>::new();
        for &gid in c.shards.iter() {
            *counts.entry(gid).or_default() += 1;
        }
        if !c.groups.is_empty() {
            let counts = c.groups.keys().map(|gid| *counts.get(gid).unwrap_or(&0));
            let min = counts.clone().min().unwrap();
            let max = counts.clone().max().unwrap();
            assert!(
                max <= min + 1,
                "imbalanced sharding, max {} too much larger than min {}: {:?}",
                max,
                min,
                c.shards,
            );
        }
    }
}
