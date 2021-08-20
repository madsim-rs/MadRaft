pub mod client;
pub mod msg;
pub mod server;
#[cfg(test)]
mod tester;
#[cfg(test)]
mod tests;

// which shard is a key in?
// please use this function,
// and please do not change it.
fn key2shard(key: &str) -> usize {
    use crate::shard_ctrler::N_SHARDS;
    key.bytes().next().unwrap_or(b'\0') as usize % N_SHARDS
}
