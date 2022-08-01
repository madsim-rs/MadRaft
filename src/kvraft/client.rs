use super::{msg::*, server::WithId};
use madsim::{
    net::{self, rpc::Request},
    rand,
    time::*,
};
use std::{
    fmt::Debug,
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering},
};

pub struct Clerk {
    core: ClerkCore<Op>,
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

pub struct ClerkCore<Req> {
    servers: Vec<SocketAddr>,
    leader: AtomicUsize,
    _mark: std::marker::PhantomData<Req>,
}

impl<Req> ClerkCore<Req>
where
    Req: Request + Clone + Debug,
    Req::Response: Debug,
{
    pub fn new(servers: Vec<SocketAddr>) -> Self {
        ClerkCore {
            servers,
            leader: AtomicUsize::new(0),
            _mark: std::marker::PhantomData,
        }
    }

    pub async fn call(&self, args: Req) -> Req::Response {
        let id: u64 = rand::random();
        let net = net::NetLocalHandle::current();
        let mut i = self.leader.load(Ordering::Relaxed);
        loop {
            debug!("[{:04x}] ->{} {:?}", id as u16, i, args);
            match net
                .call_timeout(
                    self.servers[i],
                    WithId {
                        id,
                        cmd: args.clone(),
                    },
                    Duration::from_millis(500),
                )
                .await
            {
                // client side error
                Err(e) => {
                    debug!("[{:04x}] <-{} {:?}", id as u16, i, e);
                    i = (i + 1) % self.servers.len();
                    continue;
                }
                // server side error
                Ok(Err(e)) => {
                    debug!("[{:04x}] <-{} {:?}", id as u16, i, e);
                    if let Error::NotLeader { hint } = e {
                        i = hint;
                    } else {
                        i = (i + 1) % self.servers.len();
                    }
                    continue;
                }
                Ok(Ok(v)) => {
                    debug!("[{:04x}] <-{} {:?}", id as u16, i, v);
                    self.leader.store(i, Ordering::Relaxed);
                    return v;
                }
            }
        }
    }
}
