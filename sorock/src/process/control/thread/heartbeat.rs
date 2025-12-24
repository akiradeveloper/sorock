use super::*;

pub struct Thread {
    follower_id: ServerAddress,
    ctrl_actor: Actor<Control>,
}

impl Thread {
    async fn run_once(&self) -> Result<()> {
        if !self.ctrl_actor.read().await.is_leader() {
            return Ok(());
        }

        self.ctrl_actor
            .read()
            .await
            .send_heartbeat(self.follower_id.clone())
            .await
    }

    fn run_loop(self) -> ThreadHandle {
        let fut = async move {
            let mut interval = tokio::time::interval(Duration::from_millis(300));
            loop {
                interval.tick().await;
                // Periodically sending a new commit state to the buffer.
                self.run_once().await.ok();
            }
        };
        let hdl = tokio::spawn(fut).abort_handle();
        ThreadHandle(hdl)
    }
}

pub fn run(follower_id: ServerAddress, ctrl: Actor<Control>) -> ThreadHandle {
    Thread {
        follower_id,
        ctrl_actor: ctrl,
    }
    .run_loop()
}
