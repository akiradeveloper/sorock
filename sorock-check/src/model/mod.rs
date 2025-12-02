use super::*;

mod nodes;
mod progress_log;
pub mod stream;
pub use nodes::*;
pub use progress_log::*;

pub struct Model {
    pub nodes: Arc<RwLock<Nodes>>,
    pub progress_log: Arc<RwLock<ProgressLog>>,
}
impl Model {
    pub async fn new(node: impl stream::Node + 'static) -> Self {
        let node = Arc::new(node);
        let nodes = Arc::new(RwLock::new(Nodes::default()));
        let progress_log = Arc::new(RwLock::new(ProgressLog::new()));

        tokio::spawn({
            let node = node.watch_membership().await;
            let nodes = nodes.clone();
            async move {
                stream::CopyMembership::copy(node, nodes).await;
            }
        });

        tokio::spawn({
            let node = node.clone();
            let nodes = nodes.clone();
            async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    nodes::start_copying(node.clone(), nodes.clone());
                }
            }
        });

        tokio::spawn({
            let nodes = nodes.clone();
            let progress_log = progress_log.clone();
            async move {
                loop {
                    progress_log::copy(nodes.clone(), progress_log.clone());
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        });

        Self {
            nodes,
            progress_log,
        }
    }

    pub fn test() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(Nodes::test())),
            progress_log: Arc::new(RwLock::new(ProgressLog::test())),
        }
    }
}
