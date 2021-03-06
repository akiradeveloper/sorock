use crate::*;
use bytes::BytesMut;
use std::sync::Arc;
use tokio::sync::RwLock;

#[norpc::service]
trait IOFront {
    fn create(key: String, value: Bytes) -> anyhow::Result<()>;
    fn read(key: String) -> anyhow::Result<Bytes>;
    fn sanity_check(key: String) -> anyhow::Result<usize>;
    fn set_new_cluster(cluster: ClusterMap);
}
define_client!(IOFront);

pub fn spawn(peer_out_cli: peer_out::ClientT, state: State) -> ClientT {
    use norpc::runtime::tokio::*;
    let svc = App {
        peer_out_cli,
        state,
    };
    let svc = IOFrontService::new(svc);
    let (chan, server) = ServerBuilder::new(svc).build();
    tokio::spawn(server.serve());
    IOFrontClient::new(chan)
}

pub struct State {
    cluster: RwLock<ClusterMap>,
}
impl State {
    pub fn new() -> Self {
        Self {
            cluster: RwLock::new(ClusterMap::new()),
        }
    }
}

struct App {
    peer_out_cli: peer_out::ClientT,
    state: State,
}
#[norpc::async_trait]
impl IOFront for App {
    async fn create(&self, key: String, value: Bytes) -> anyhow::Result<()> {
        use reed_solomon_erasure::galois_8::ReedSolomon;

        let plen = value.len() / K;
        let r = ReedSolomon::new(K, N - K).unwrap();
        let mut data = vec![];
        for i in 0..K {
            let buf = value.slice(i * plen..(i + 1) * plen);
            data.push(buf);
        }
        let mut parity = vec![];
        let zero = vec![0; plen];
        for _ in 0..(N - K) {
            let mut buf = BytesMut::with_capacity(plen);
            buf.extend_from_slice(&zero);
            parity.push(buf);
        }
        r.encode_sep(&data, &mut parity).unwrap();

        let mut piece_data = vec![];
        data.reverse();
        for _ in 0..K {
            piece_data.push(data.pop().unwrap());
        }
        parity.reverse();
        for _ in 0..(N - K) {
            piece_data.push(parity.pop().unwrap().freeze());
        }
        piece_data.reverse();

        let holders = self
            .state
            .cluster
            .read()
            .await
            .compute_holders(key.clone(), N);
        let cluster_version = self.state.cluster.read().await.version();
        let mut futs = vec![];
        for i in 0..N {
            let data = piece_data.pop().unwrap();
            let key = key.clone();
            let uri = holders[i as usize].clone();
            let mut out_cli = self.peer_out_cli.clone();
            futs.push(async move {
                let version = cluster_version;
                let loc = PieceLocator {
                    key,
                    index: i as u8,
                };
                match uri {
                    Some(uri) => {
                        let res = out_cli
                            .send_piece(
                                uri,
                                SendPiece {
                                    version,
                                    loc,
                                    data: Some(data),
                                },
                            )
                            .await;
                        res.is_ok()
                    }
                    None => false,
                }
            });
        }
        let stream = futures::stream::iter(futs);
        let mut buffered = stream.buffer_unordered(N);
        let mut n_ok = 0;
        while let Some(send_ok) = buffered.next().await {
            if send_ok {
                n_ok += 1;
            }
        }
        if n_ok < K {
            anyhow::bail!("failed to write sufficient pieces: key={}", &key);
        }
        Ok(())
    }
    async fn read(&self, key: String) -> anyhow::Result<Bytes> {
        let peer_out_cli = self.peer_out_cli.clone();
        let cluster = self.state.cluster.read().await.clone();
        let rebuild = rebuild::Rebuild {
            peer_out_cli,
            cluster: cluster.clone(),
            with_parity: false,
            fallback_broadcast: true,
        };
        let pieces = rebuild.rebuild(key.clone()).await?;
        let mut merged = BytesMut::new();
        for i in 0..K {
            let piece_data = &pieces[i];
            merged.extend_from_slice(piece_data);
        }
        return Ok(merged.freeze());
    }
    async fn sanity_check(&self, key: String) -> anyhow::Result<usize> {
        let cluster = self.state.cluster.read().await.clone();
        let holders = cluster.compute_holders(key.clone(), N);
        let mut futs = vec![];
        for i in 0..N {
            let holder = &holders[i];
            match holder {
                None => {}
                Some(holder) => {
                    let mut peer_out_cli = self.peer_out_cli.clone();
                    let holder = holder.clone();
                    let loc = PieceLocator {
                        key: key.clone(),
                        index: i as u8,
                    };
                    let fut = async move {
                        let found = peer_out_cli.piece_exists(holder, loc).await?;
                        Ok::<bool, anyhow::Error>(found)
                    };
                    let fut = tokio::time::timeout(std::time::Duration::from_secs(5), fut);
                    futs.push(fut);
                }
            }
        }
        let n_should_found = futs.len();
        let stream = futures::stream::iter(futs);
        let mut buffered = stream.buffer_unordered(N);
        let mut n_found = 0;
        while let Some(rep) = buffered.next().await {
            if rep.is_err() {
                continue;
            }
            let rep = rep.unwrap();
            if rep.is_err() {
                continue;
            }
            let rep = rep.unwrap();
            if rep {
                n_found += 1;
            }
        }
        let n_lost = n_should_found - n_found;
        Ok(n_lost)
    }
    async fn set_new_cluster(&self, cluster: ClusterMap) {
        *self.state.cluster.write().await = cluster;
    }
}
