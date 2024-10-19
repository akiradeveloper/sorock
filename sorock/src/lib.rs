#![deny(unused_must_use)]

/// Implementation of `RaftProcess`.
pub mod process;

mod error;
mod node;
use error::Error;

pub mod backend;
/// Implementation of gRPC services.
pub mod service;

use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
pub use node::{RaftDriver, RaftNode};
use process::RaftProcess;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Uri;

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
