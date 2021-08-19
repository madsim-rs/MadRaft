pub mod client;
pub mod msg;
pub mod server;
#[cfg(test)]
mod tester;
#[cfg(test)]
mod tests;

pub const N_SHARDS: usize = 10;
