use super::*;

struct Thread {
    pending_queue: query_queue::QueryQueue,
    query_exec_actor: Actor<query_queue::QueryExec>,
    driver: node::RaftHandle,
}

impl Thread {
    async fn run_once(&self) -> Result<()> {
        let current_pending_qs = self.pending_queue.lock().drain();
        if current_pending_qs.is_empty() {
            return Ok(());
        }

        let conn = self.driver.connect(self.driver.self_node_id.clone());
        if let Some(read_index) = conn.issue_read_index().await? {
            self.query_exec_actor
                .write()
                .await
                .register(read_index, current_pending_qs);
        } else {
            self.pending_queue.lock().requeue(current_pending_qs);
        }

        Ok(())
    }

    fn do_loop(self) -> ThreadHandle {
        let fut = async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                self.run_once().await.ok();
            }
        };
        let hdl = tokio::spawn(fut).abort_handle();
        ThreadHandle(hdl)
    }
}

pub fn new(
    pending_queue: query_queue::QueryQueue,
    exec_queue: Actor<query_queue::QueryExec>,
    driver: node::RaftHandle,
) -> ThreadHandle {
    Thread {
        pending_queue,
        query_exec_actor: exec_queue,
        driver,
    }
    .do_loop()
}
