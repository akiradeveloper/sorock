use super::*;

use std::collections::{HashMap, HashSet};

pub struct ShardTable {
    fwd: HashMap<NodeAddress, HashSet<ShardIndex>>,
    back: HashMap<ShardIndex, HashSet<NodeAddress>>,
}

impl ShardTable {
    pub fn new() -> Self {
        Self {
            fwd: HashMap::new(),
            back: HashMap::new(),
        }
    }

    fn remove_mapping(&mut self, node_id: &NodeAddress) {
        if let Some(shards) = self.fwd.remove(node_id) {
            for shard in shards {
                if let Some(nodes) = self.back.get_mut(&shard) {
                    nodes.remove(node_id);
                    if nodes.is_empty() {
                        self.back.remove(&shard);
                    }
                }
            }
        }
    }

    pub fn update_mapping(&mut self, node_id: NodeAddress, mapping: Vec<ShardIndex>) {
        self.remove_mapping(&node_id);

        self.fwd
            .entry(node_id.clone())
            .or_insert_with(HashSet::new)
            .extend(mapping.iter().cloned());

        for shard in mapping {
            self.back
                .entry(shard)
                .or_insert_with(HashSet::new)
                .insert(node_id.clone());
        }
    }

    pub fn choose_one_replica(&self, shard_index: ShardIndex) -> Option<NodeAddress> {
        self.back
            .get(&shard_index)
            .and_then(|nodes| nodes.iter().next().cloned())
    }
}
