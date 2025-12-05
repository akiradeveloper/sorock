use super::*;

pub struct Effect {
    pub peers: Peers,
}

impl Effect {
    pub fn exec(self, init_next_index: Index) {
        let mut peer_contexts = self.peers.peer_contexts.write();
        for (_, peer) in peer_contexts.iter_mut() {
            peer.progress = ReplicationProgress::new(init_next_index);
        }
    }
}
