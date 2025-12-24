use super::*;

struct Thread {
    query_queue: query_queue::QueryQueue,
    query_exec_actor: Actor<query_queue::QueryExec>,
    io: node::RaftIO,
}

impl Thread {
    async fn run_once(&self) -> Result<()> {
        let current_pending_qs = self.query_queue.lock().drain();
        if current_pending_qs.is_empty() {
            return Ok(());
        }

        let conn = self.io.connect(self.io.self_server_id.clone());

        info!("ask for read_index to leader");
        if let Some(read_index) = conn.issue_read_index().await? {
            info!("got read_index from leader: {}", read_index);
            self.query_exec_actor
                .write()
                .await
                .register(read_index, current_pending_qs);
        } else {
            self.query_queue.lock().requeue(current_pending_qs);
        }

        Ok(())
    }

    fn run_loop(self) -> ThreadHandle {
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

pub fn run(
    query_queue: query_queue::QueryQueue,
    query_exec_actor: Actor<query_queue::QueryExec>,
    io: node::RaftIO,
) -> ThreadHandle {
    Thread {
        query_queue,
        query_exec_actor,
        io,
    }
    .run_loop()
}
