use super::*;

pub type Error = std::io::Error;
pub type Result<T> = std::io::Result<T>;
pub type SnapshotStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<Bytes>> + Send>>;