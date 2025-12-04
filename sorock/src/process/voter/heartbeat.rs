use super::*;

impl Voter {
    pub async fn send_heartbeat(&self, follower_id: NodeId) -> Result<()> {
        let ballot = self.read_ballot().await?;
        let leader_commit_index = self.command_log.commit_pointer.load(Ordering::SeqCst);
        let req = request::Heartbeat {
            leader_term: ballot.cur_term,
            leader_commit_index,
        };
        let conn = self.driver.connect(follower_id);
        conn.queue_heartbeat(req);
        Ok(())
    }
}
