use super::msg::*;
use crate::kvraft::server::{Server, State};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type ShardCtrler = Server<ShardInfo>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ShardInfo {
    /// The index equals the config number, start from 0.
    config: Vec<Config>,
    /// A circular queue with max capacity 50
    ids: Vec<u64>,
}

impl Default for ShardInfo {
    fn default() -> Self {
        ShardInfo {
            config: vec![Config::default()],
            ids: vec![],
        }
    }
}

impl State for ShardInfo {
    type Command = Op;

    fn apply(&mut self, id: u64, cmd: Self::Command) -> Option<Config> {
        let unique = !self.ids.contains(&id);
        if self.ids.len() > 50 {
            self.ids.remove(0);
        }
        self.ids.push(id);
        match cmd {
            Op::Query { num } => {
                let num = (num as usize).min(self.config.len() - 1);
                return Some(self.config[num].clone());
            }
            Op::Join { groups } if unique => {
                let mut cfg = self.config.last().unwrap().clone();
                cfg.num += 1;
                cfg.groups.extend(groups);
                cfg.rebalance();
                self.config.push(cfg);
            }
            Op::Leave { gids } if unique => {
                let mut cfg = self.config.last().unwrap().clone();
                cfg.num += 1;
                for gid in gids {
                    cfg.groups.remove(&gid);
                }
                cfg.rebalance();
                self.config.push(cfg);
            }
            Op::Move { shard, gid } if unique => {
                let mut cfg = self.config.last().unwrap().clone();
                cfg.num += 1;
                cfg.shards[shard] = gid;
                self.config.push(cfg);
            }
            _ => {}
        }
        None
    }
}

impl Config {
    fn rebalance(&mut self) {
        if self.groups.is_empty() {
            return;
        }
        let min_shards_per_group = self.shards.len() / self.groups.len();
        let max_shards_count = self.shards.len() % self.groups.len();
        // WARN: DO NOT use HashMap because it iteration order is not deterministic.
        let mut remain: BTreeMap<Gid, usize> = self
            .groups
            .keys()
            .map(|&gid| (gid, min_shards_per_group))
            .collect();
        remain
            .values_mut()
            .take(max_shards_count)
            .for_each(|c| *c += 1);

        for gid in &mut self.shards {
            if let Some(count) = remain.get_mut(gid) {
                if *count > 0 {
                    *count -= 1;
                    continue;
                }
            }
            let (&g, count) = remain.iter_mut().find(|(_, count)| **count > 0).unwrap();
            *gid = g;
            *count -= 1;
        }
    }
}
