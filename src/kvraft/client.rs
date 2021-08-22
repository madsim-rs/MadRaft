use super::msg::*;
use madsim::{net, time::*};
use std::net::SocketAddr;

pub struct Clerk {
    core: ClerkCore<Op, String>,
}

impl Clerk {
    pub fn new(servers: Vec<SocketAddr>) -> Clerk {
        Clerk {
            core: ClerkCore::new(servers),
        }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    pub async fn get(&self, key: String) -> String {
        self.core.call(Op::Get { key }).await
    }

    pub async fn put(&self, key: String, value: String) {
        self.core.call(Op::Put { key, value }).await;
    }

    pub async fn append(&self, key: String, value: String) {
        self.core.call(Op::Append { key, value }).await;
    }
}

pub struct ClerkCore<Req, Rsp> {
    servers: Vec<SocketAddr>,
    _mark: std::marker::PhantomData<(Req, Rsp)>,
}

impl<Req, Rsp> ClerkCore<Req, Rsp>
where
    Req: net::Message + Clone,
    Rsp: net::Message,
{
    pub fn new(servers: Vec<SocketAddr>) -> Self {
        ClerkCore {
            servers,
            _mark: std::marker::PhantomData,
        }
    }

    pub async fn call(&self, args: Req) -> Rsp {
        let net = net::NetLocalHandle::current();
        for i in 0..self.servers.len() {
            let ret = net
                .call_timeout::<Req, Result<Rsp, Error>>(
                    self.servers[i],
                    args.clone(),
                    Duration::from_millis(500),
                )
                .await;
            todo!("handle RPC results");
        }
        todo!("handle RPC results");
    }
}
