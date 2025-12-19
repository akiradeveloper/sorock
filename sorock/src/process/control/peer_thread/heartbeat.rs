use super::*;

#[derive(Clone)]
pub struct Thread {
    follower_id: NodeAddress,
    ctrl: Read<Control>,
}
impl Thread {
    async fn run_once(&self) -> Result<()> {
        let election_state = self.ctrl.read_election_state();
        ensure!(std::matches!(
            election_state,
            control::ElectionState::Leader
        ));

        self.ctrl.send_heartbeat(self.follower_id.clone()).await
    }

    fn do_loop(self) -> thread::ThreadHandle {
        let fut = async move {
            let mut interval = tokio::time::interval(Duration::from_millis(300));
            loop {
                interval.tick().await;
                // Periodically sending a new commit state to the buffer.
                self.run_once().await.ok();
            }
        };
        let hdl = tokio::spawn(fut).abort_handle();
        thread::ThreadHandle(hdl)
    }
}

pub fn new(follower_id: NodeAddress, ctrl: Read<Control>) -> thread::ThreadHandle {
    Thread { follower_id, ctrl }.do_loop()
}
