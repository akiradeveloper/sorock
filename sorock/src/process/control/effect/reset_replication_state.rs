use super::*;

pub struct Effect<'a> {
    pub ctrl: &'a mut Control,
}

impl Effect<'_> {
    pub async fn exec(self, init_next_index: LogIndex) {
        for (_, cur_progress) in &mut self.ctrl.replication_progresses {
            *cur_progress.lock().await = ReplicationProgress::new(init_next_index);
        }
    }
}
