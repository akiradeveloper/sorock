use crate::{Id, RaftCore, RaftApp};
use tokio::net::UdpSocket;
use tokio::net::ToSocketAddrs;
use std::sync::Arc;

#[derive(serde::Serialize, serde::Deserialize)]
pub enum Message {
    Heartbeat {
        term: u64,
        leader_id: String,
        leader_commit: u64,
    }
}

pub struct Server<A: RaftApp> {
    core: Arc<RaftCore<A>>,
}
impl <A: RaftApp> Server<A> {
    pub async fn new(core: Arc<RaftCore<A>>) -> tokio::io::Result<Self> {
        Ok(Self {
            core,
        })
    }
    pub async fn serve<Addr: ToSocketAddrs>(self, addr: Addr) -> anyhow::Result<()> {
        let Server {
            core
        } = self;

        let mut socket = tokio::net::UdpSocket::bind(addr).await?;
        let mut buf = vec![0; 1_000];

        loop {
            let (n_written, _sender) = socket.recv_from(&mut buf).await?;
            let res = rmp_serde::from_slice(&buf[0..n_written]);
            if res.is_err() {
                log::warn!("received message is broken for some reason. failed to decode.");
                continue;
            }
            match res.unwrap() {
                Message::Heartbeat { term, leader_id, leader_commit } => {
                    let _ = core.receive_heartbeat(leader_id, term, leader_commit).await;
                },
            }
        }
    }
}

pub struct Client {
    socket: UdpSocket,
}
impl Client {
    pub async fn new(id: Id) -> anyhow::Result<Self> { 
        let url = url::Url::parse(&id)?;
        let sock = format!("{}:0", url.host_str().unwrap());
        // let sock = tokio::net::lookup_host(sock).await.unwrap().next().unwrap();
        // dbg!(&sock);
        Ok(Self {
            socket: tokio::net::UdpSocket::bind(sock).await?,
        })
    }
    pub async fn send_to(&mut self, to: Id, message: Message) -> anyhow::Result<()> {
        let data = rmp_serde::to_vec(&message)?;
        let url = url::Url::parse(&to)?;
        let sock = format!("{}:{}", url.host_str().unwrap(), url.port().unwrap());
        // let sock = tokio::net::lookup_host(sock).await.unwrap().next().unwrap();
        // dbg!(&sock);
        self.socket.send_to(&data, sock).await?;
        Ok(())
    }
}