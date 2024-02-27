use super::*;

use futures::stream::TryStreamExt;
use futures::Stream;
use futures::StreamExt;
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

    pub async fn write<W: AsyncWrite + Unpin>(
        writer: W,
        st: impl Stream<Item = io::Result<Bytes>> + Unpin,
    ) -> io::Result<()> {
        let st = st.map(|res| res.map_err(|_| std::io::Error::from(std::io::ErrorKind::Other)));
        read_bytes_stream(writer, st).await
    }
}
pub use writer::write;
