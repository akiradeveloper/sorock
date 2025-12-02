use super::*;

use tokio::task::AbortHandle;
struct DropHandle(AbortHandle);
impl Drop for DropHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Default)]
pub struct LogState {
    pub head_index: u64,
    pub snapshot_index: u64,
    pub app_index: u64,
    pub commit_index: u64,
    pub last_index: u64,
}

#[derive(Default)]
pub struct NodeState {
    pub log_state: LogState,
    drop_log_metrics_stream: Option<DropHandle>,
}

#[derive(Default)]
pub struct Nodes {
    pub nodes: HashMap<Uri, NodeState>,
}
impl Nodes {
    pub async fn update_membership(&mut self, new_membership: HashSet<Uri>) {
        let mut del_list = vec![];
        for (uri, _) in self.nodes.iter() {
            if !new_membership.contains(uri) {
                del_list.push(uri.clone());
            }
        }
        for uri in del_list {
            self.nodes.remove(&uri);
        }
        for uri in new_membership {
            self.nodes.entry(uri.clone()).or_default();
        }
    }

    pub fn test() -> Self {
        let mut out = HashMap::new();
        out.insert(
            Uri::from_static("http://n1:3000"),
            NodeState {
                log_state: model::LogState {
                    head_index: 100,
                    snapshot_index: 110,
                    app_index: 140,
                    commit_index: 160,
                    last_index: 165,
                },
                drop_log_metrics_stream: None,
            },
        );
        out.insert(
            Uri::from_static("http://n2:3000"),
            NodeState {
                log_state: model::LogState {
                    head_index: 125,
                    snapshot_index: 130,
                    app_index: 140,
                    commit_index: 165,
                    last_index: 180,
                },
                drop_log_metrics_stream: None,
            },
        );
        out.insert(
            Uri::from_static("http://n3:3000"),
            NodeState {
                log_state: model::LogState {
                    head_index: 168,
                    snapshot_index: 168,
                    app_index: 168,
                    commit_index: 168,
                    last_index: 168,
                },
                drop_log_metrics_stream: None,
            },
        );
        Self { nodes: out }
    }
}

/// Start data fetching for each node.
pub fn start_copying(node: Arc<dyn stream::Node>, nodes: Arc<RwLock<Nodes>>) {
    let mut data = nodes.write();
    for (url, state) in &mut data.nodes {
        if state.drop_log_metrics_stream.is_none() {
            let hdl = tokio::spawn({
                let url = url.clone();
                let node = node.clone();
                let data = nodes.clone();
                async move {
                    stream::CopyLogMetrics { url: url.clone() }
                        .copy(node.watch_log_metrics(url).await, data)
                        .await;
                }
            })
            .abort_handle();
            state.drop_log_metrics_stream = Some(DropHandle(hdl));
        }
    }
}
