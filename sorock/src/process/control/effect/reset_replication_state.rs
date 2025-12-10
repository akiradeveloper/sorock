use super::*;

pub struct Effect {
    pub ctrl: Control,
}

impl Effect {
    pub fn exec(self, init_next_index: LogIndex) {
        let mut peer_contexts = self.ctrl.peer_contexts.write();
        for (_, peer) in peer_contexts.iter_mut() {
            peer.progress = ReplicationProgress::new(init_next_index);
        }
    }
}
