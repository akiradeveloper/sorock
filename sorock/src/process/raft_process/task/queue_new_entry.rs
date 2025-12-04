use super::*;

pub struct Task {
    pub command_log: CommandLog,
    pub voter: Ref<Voter>,
    pub peers: PeerSvc,
    pub queue_tx: thread::EventProducer<thread::QueueEvent>,
    pub replication_tx: thread::EventProducer<thread::ReplicationEvent>,
}
impl Task {
    pub async fn exec(
        &self,
        command: Bytes,
        completion: Completion,
    ) -> Result<Index> {
        ensure!(self.voter.allow_queue_new_entry().await?);

        let append_index = self
            .command_log
            .append_new_entry(command.clone(), None)
            .await?;

        self.command_log
            .register_completion(append_index, completion);

        process_configuration_command::Task {
            peers: self.peers.clone(),
            voter: self.voter.clone(),
        }.exec(&command, append_index).await?;

        self.queue_tx.push_event(thread::QueueEvent);
        self.replication_tx.push_event(thread::ReplicationEvent);

        Ok(append_index)
    }
}