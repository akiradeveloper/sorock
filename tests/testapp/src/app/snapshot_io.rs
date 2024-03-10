use super::*;

use futures::stream::TryStreamExt;
use futures::Stream;
use tokio::io;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec;

mod reader {
    use super::*;

    fn into_bytes_stream<R>(r: R) -> impl Stream<Item = io::Result<Bytes>>
    where
        R: AsyncRead,
    {
        codec::FramedRead::new(r, codec::BytesCodec::new()).map_ok(|bytes| bytes.freeze())
    }

    /// Read a stream from `reader`.
    pub fn read<R: AsyncRead>(reader: R) -> impl Stream<Item = io::Result<Bytes>> {
        into_bytes_stream(reader)
    }
}
pub use reader::read;

mod writer {
    use super::*;

    async fn read_bytes_stream<W: AsyncWrite + Unpin>(
        w: W,
        mut st: impl Stream<Item = io::Result<Bytes>> + Unpin,
    ) -> io::Result<()> {
        use futures::SinkExt;
        let mut sink = codec::FramedWrite::new(w, codec::BytesCodec::new());
        sink.send_all(&mut st).await?;
        Ok(())
    }

    /// Write `stream` to `writer`.
    pub async fn write<W: AsyncWrite + Unpin>(
        writer: W,
        stream: impl Stream<Item = io::Result<Bytes>> + Unpin,
    ) -> io::Result<()> {
        read_bytes_stream(writer, stream).await
    }
}
pub use writer::write;
