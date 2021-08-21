use super::{key2shard, msg::*};
use crate::kvraft::client::ClerkCore;
use crate::shard_ctrler::{client::Clerk as CtrlerClerk, msg::Config};
use madsim::time::*;
use std::{net::SocketAddr, sync::Mutex};

pub struct Clerk {
    ctrl_ck: CtrlerClerk,
    config: Mutex<Config>,
}

impl Clerk {
    pub fn new(servers: Vec<SocketAddr>) -> Clerk {
        Clerk {
            ctrl_ck: CtrlerClerk::new(servers),
            config: Mutex::new(Config::default()),
        }
    }

    pub async fn get(&self, key: String) -> String {
        match self.call(Op::Get { key }).await {
            Reply::Get { value } => value.unwrap_or_default(),
            _ => unreachable!(),
        }
    }

    pub async fn put(&self, key: String, value: String) {
        self.call(Op::Put { key, value }).await;
    }

    pub async fn append(&self, key: String, value: String) {
        self.call(Op::Append { key, value }).await;
    }

    async fn call(&self, args: Op) -> Reply {
        let shard = key2shard(args.key());
        for iter in 0..100 {
            let config = if iter == 0 {
                self.config.lock().unwrap().clone()
            } else {
                // ask controler for the latest configuration.
                let config = self.ctrl_ck.query().await;
                *self.config.lock().unwrap() = config.clone();
                config
            };
            if config.num == 0 {
                sleep(Duration::from_millis(100)).await;
                continue;
            }
            let gid = config.shards[shard];
            let ck = ClerkCore::<Op, Reply>::new(config.groups[&gid].clone());
            match timeout(Duration::from_secs(1), ck.call(args.clone())).await {
                Err(_) | Ok(Reply::WrongGroup) => continue,
                Ok(r) => return r,
            }
        }
        panic!("failed to call {:?}", args);
    }
}
