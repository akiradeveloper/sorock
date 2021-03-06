#![deny(unused_must_use)]

use anyhow::Result;
use bytes::Bytes;
use lol_core::Uri;

#[macro_export]
macro_rules! define_client {
    ($name: ident) => {
        paste::paste! {
            pub type ClientT = [<$name Client>]<norpc::runtime::tokio::Channel<[<$name Request>], [<$name Response>] >>;
        }
    };
}

pub mod cluster_in;
mod cluster_map;
pub mod io_front;
pub mod peer_in;
pub mod peer_out;
pub mod piece_store;
pub mod rebuild_queue;
pub mod stabilizer;
pub mod storage_service;
use cluster_map::ClusterMap;
mod rebuild;

pub mod raft_service;

use futures::stream::StreamExt;

pub mod proto_compiled {
    tonic::include_proto!("sorock");
}

/// Number of data chunks
pub const K: usize = 4;
/// Number of data + parity chunks
pub const N: usize = 8;

#[derive(serde::Serialize, serde::Deserialize)]
enum Command {
    AddNode { uri: URI, cap: f64 },
    RemoveNode { uri: URI },
}
impl Command {
    fn encode(&self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }
    fn decode(b: &[u8]) -> Self {
        bincode::deserialize(b).unwrap()
    }
}

#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub struct PieceLocator {
    pub key: String,
    pub index: u8,
}

pub struct SendPiece {
    pub version: u64,
    pub loc: PieceLocator,
    pub data: Option<Bytes>,
}
#[derive(thiserror::Error, Debug)]
pub enum SendPieceError {
    #[error("send-piece with older version was rejected.")]
    Rejected,
    #[error("failed any way")]
    Failed,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Hash, PartialEq, Eq)]
struct URI(#[serde(with = "http_serde::uri")] tonic::transport::Uri);
