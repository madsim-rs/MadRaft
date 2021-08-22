use crate::shard_ctrler::client::Clerk as CtrlerClerk;
use std::net::SocketAddr;

pub struct Clerk {
    ctrl_ck: CtrlerClerk,
}

impl Clerk {
    pub fn new(servers: Vec<SocketAddr>) -> Clerk {
        Clerk {
            ctrl_ck: CtrlerClerk::new(servers),
        }
    }

    pub async fn get(&self, key: String) -> String {
        todo!("get");
    }

    pub async fn put(&self, key: String, value: String) {
        todo!("put");
    }

    pub async fn append(&self, key: String, value: String) {
        todo!("append");
    }
}
