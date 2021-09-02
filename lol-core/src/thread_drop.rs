use tokio::task::JoinHandle;

/// Mechanism to drop spawned threads when the owner drops.
#[derive(Debug)]
pub struct ThreadDrop {
    handles: Vec<JoinHandle<()>>,
}
impl ThreadDrop {
    pub fn new() -> Self {
        Self { handles: vec![] }
    }
    pub fn register_abort_on_drop(&mut self, hdl: JoinHandle<()>) {
        self.handles.push(hdl);
    }
}
impl Drop for ThreadDrop {
    fn drop(&mut self) {
        for hdl in &mut self.handles {
            hdl.abort();
        }
    }
}
#[tokio::test]
async fn test_thread_drop() {
    use std::time::Duration;
    let hdl = tokio::spawn(async {
        loop {
            println!("I am alive!!!");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut td = ThreadDrop::new();
    td.register_abort_on_drop(hdl);
    drop(td);
    println!("Dropped. It should be silent now.");
    tokio::time::sleep(Duration::from_secs(1)).await;
}
