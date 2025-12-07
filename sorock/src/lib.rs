#![deny(unused_must_use)]

/// Implementation of `RaftProcess`.
mod error;
use error::Error;

/// Implementation fo `RaftProcess`.
pub mod process;

/// Implementation of gRPC services.
pub mod service;

/// Implementation of Raft node containing multiple Raft processes.
pub mod node;

use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use derive_more::{Deref, Display, FromStr};
use futures::Stream;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Uri;

/// Identifier of Raft server.
#[derive(
    serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, Hash, Debug, Display, FromStr,
)]
pub struct NodeAddress(#[serde(with = "http_serde::uri")] Uri);

impl NodeAddress {
    pub fn new(uri: Uri) -> Self {
        Self(uri)
    }
}

/// Identifier of Shard.
pub type ShardIndex = u32;
