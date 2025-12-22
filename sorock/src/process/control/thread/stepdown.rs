use super::*;

#[derive(Clone)]
pub struct Thread {
    ctrl: Actor<Control>,
}

impl Thread {
    pub async fn run_once(&self) -> Result<()> {
        control::effect::try_stepdown::Effect {
            ctrl: &mut *self.ctrl.write().await,
        }
        .exec()
        .await?;

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

pub fn new(ctrl: Actor<Control>) -> ThreadHandle {
    Thread { ctrl }.do_loop()
}
