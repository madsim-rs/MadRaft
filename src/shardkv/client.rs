use super::msg::*;
use crate::shard_ctrler::{client::Clerk as CtrlerClerk, msg::Config, N_SHARDS};
use madsim::{
    net,
    rand::{self, Rng},
    time::*,
};
use std::{net::SocketAddr, sync::Mutex};

// which shard is a key in?
// please use this function,
// and please do not change it.
pub fn key2shard(key: &str) -> usize {
    key.bytes().next().unwrap_or(b'\0') as usize % N_SHARDS
}

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
        self.call(Op::Get { key }).await
    }

    pub async fn put(&self, key: String, value: String) {
        self.call(Op::Put { key, value }).await;
    }

    pub async fn append(&self, key: String, value: String) {
        self.call(Op::Append { key, value }).await;
    }

    async fn call(&self, args: Op) -> String {
        let id: u64 = rand::rng().gen();
        let shard = key2shard(args.key());
        let net = net::NetLocalHandle::current();
        loop {
            let config = self.config.lock().unwrap().clone();
            let gid = config.shards[shard];
            let g0 = vec![];
            let group = config.groups.get(&gid).unwrap_or(&g0);
            for (i, &addr) in group.iter().enumerate() {
                debug!("->{} {:?}", i, args);
                match net
                    .call_timeout::<(u64, Op), Result<String, Error>>(
                        addr,
                        (id, args.clone()),
                        Duration::from_millis(500),
                    )
                    .await
                {
                    // client side error
                    Err(e) => {
                        debug!("<-{} {:?}", i, e);
                        continue;
                    }
                    // server side error
                    Ok(Err(e)) => {
                        debug!("<-{} {:?}", i, e);
                        if e == Error::WrongGroup {
                            break;
                        }
                        continue;
                    }
                    Ok(Ok(v)) => {
                        debug!("<-{} ok", i);
                        return v;
                    }
                }
            }
            sleep(Duration::from_millis(100)).await;
            // ask controler for the latest configuration.
            let config = self.ctrl_ck.query().await;
            *self.config.lock().unwrap() = config;
        }
    }
}
