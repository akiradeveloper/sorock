#![deny(unused_must_use)]

pub mod process;

pub mod client;
mod communicator;
mod error;
mod node;
pub mod raft_service;
use error::Error;

use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
pub use node::{RaftDriver, RaftNode};
use process::RaftProcess;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Uri;
pub mod reflection_service;

mod generated {
    pub mod lolraft;
}

mod raft {
    pub use super::generated::lolraft::*;
    pub type RaftClient = raft_client::RaftClient<tonic::transport::channel::Channel>;
}

/// Identifier of `RaftNode`.
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Debug,
    derive_more::Display,
    derive_more::FromStr,
)]
pub struct NodeId(#[serde(with = "http_serde::uri")] Uri);

impl NodeId {
    pub fn new(uri: Uri) -> Self {
        Self(uri)
    }
}

/// Identifier of Shard.
pub type ShardId = u32;
