use super::*;

#[derive(Clone)]
pub struct Thread {
    voter: Voter,
}

impl Thread {
    pub async fn run_once(&self) -> Result<()> {
        self.voter.try_stepdown().await?;
        Ok(())
    }

    pub fn do_loop(self) -> ThreadHandle {
        let fut = async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                self.run_once().await.ok();
            }
        };
        let hdl = tokio::spawn(fut).abort_handle();
        ThreadHandle(hdl)
    }
}

pub fn new(voter: Voter) -> ThreadHandle {
    Thread { voter }.do_loop()
}
