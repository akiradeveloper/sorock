use clap::Clap;
use std::io;
use termion::{event::Key, input::MouseTerminal, raw::IntoRawMode, screen::AlternateScreen};
use tui::{backend::TermionBackend, Terminal};
mod event;
use event::Event;
mod app;
mod ui;
use app::App;
use futures::stream;
use futures::StreamExt;
use lol_core::connection::{self, gateway};
use lol_core::proto_compiled::raft_client::RaftClient;
use lol_core::{core_message, proto_compiled};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::sync::watch;
use tonic::transport::channel::Endpoint;

#[derive(Clap)]
struct Opts {
    id: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts: Opts = Opts::parse();

    let stdout = io::stdout().into_raw_mode()?;
    let stdout = MouseTerminal::from(stdout);
    let stdout = AlternateScreen::from(stdout);
    let backend = TermionBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let events = event::Events::new();

    let connector = lol_core::gateway::Connector::new(|id| { Endpoint::from_shared(id).unwrap() });
    let gateway = connector.connect(opts.id);

    let data_stream_0 = stream::unfold(gateway.clone(), |gateway| async move {
        let mut cli = RaftClient::new(gateway.clone());

        let cluster_info = cli.request_cluster_info(proto_compiled::ClusterInfoReq {}).await.ok()?.into_inner();

        let endpoints = cluster_info.membership.clone();
        let mut futs = vec![];
        for id in endpoints.clone() {
            let fut = async move {
                let res: anyhow::Result<_> = {
                    let msg = core_message::Req::LogInfo;
                    let req = proto_compiled::ProcessReq {
                        message: core_message::Req::serialize(&msg),
                        core: true,
                    };
                    let endpoint = Endpoint::from_shared(id.clone())
                        .unwrap()
                        .timeout(Duration::from_secs(3));
                    let mut conn = RaftClient::connect(endpoint).await?;
                    let res = conn.request_process_locally(req).await?.into_inner();
                    let msg = core_message::Rep::deserialize(&res.message).unwrap();
                    if let core_message::Rep::LogInfo {
                        snapshot_index,
                        last_applied,
                        commit_index,
                        last_log_index,
                    } = msg
                    {
                        Ok(app::LogInfo {
                            snapshot_index,
                            last_applied,
                            commit_index,
                            last_log_index,
                        })
                    } else {
                        unreachable!()
                    }
                };
                res
            };
            futs.push(fut);
        }
        let results = futures::future::join_all(futs).await;

        let mut h = HashMap::new();
        for (id, x) in endpoints.into_iter().zip(results) {
            match x {
                Ok(log_info) => {
                    h.insert(id, app::NodeStatus {
                        log_info: Some(log_info),
                        health_ok: true,
                    });
                },
                Err(_) => {
                    h.insert(id, app::NodeStatus::default());
                }
            }
        }

        let mut res = app::ClusterStatus::default();
        res.leader_id = cluster_info.leader_id;
        res.data = h;
        Some((res, gateway))
    });
    let (tx1, data_stream_1) = watch::channel(app::ClusterStatus {
        leader_id: None,
        data: HashMap::new(),
    });
    tokio::spawn(async move {
        tokio::pin!(data_stream_0);
        while let Some(x) = data_stream_0.next().await {
            tx1.send(x);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
    let it = std::iter::repeat_with(|| data_stream_1.borrow().clone());
    let data_stream = async_stream::stream! {
        for x in it.into_iter() {
            yield x
        }
    };
    tokio::pin!(data_stream);

    let mut app = App::new(data_stream).await;
    loop {
        if !app.running {
            break;
        }

        let model = app.make_model().await;
        terminal.draw(|f| ui::draw(f, model));

        tokio::time::sleep(Duration::from_millis(100)).await;

        if let Ok(evt) = events.next() {
            match evt {
                Event::Input(key) => match key {
                    Key::Char(c) => app.on_key(c),
                    _ => {}
                },
            }
        }
    }
    Ok(())
}