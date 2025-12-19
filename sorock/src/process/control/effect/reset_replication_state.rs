use super::*;

pub struct Effect {
    pub ctrl: Control,
}

impl Effect {
    pub fn exec(self, init_next_index: LogIndex) {
        let mut progresses = self.ctrl.replication_progresses.write();
        for (_, cur_progress) in progresses.iter_mut() {
            *cur_progress = ReplicationProgress::new(init_next_index);
        }
    }
}
