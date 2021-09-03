use bytes::Bytes;
use futures::stream::{Stream, StreamExt, TryStreamExt};
use tokio::io::{AsyncRead, AsyncWrite, Result};
use tokio_util::codec;

fn into_bytes_stream<R>(r: R) -> impl Stream<Item = Result<Bytes>>
where
	R: AsyncRead,
{
	codec::FramedRead::new(r, codec::BytesCodec::new()).map_ok(|bytes| bytes.freeze())
}
pub fn into_snapshot_stream<R: AsyncRead>(
	reader: R,
) -> impl Stream<Item = anyhow::Result<Bytes>> {
	into_bytes_stream(reader).map(|res| res.map_err(|_| anyhow::Error::msg("streaming error")))
}
async fn read_bytes_stream<W: AsyncWrite + Unpin>(
	w: W,
	mut st: impl Stream<Item = Result<Bytes>> + Unpin,
) -> anyhow::Result<()> {
	use futures::SinkExt;
	let mut sink = codec::FramedWrite::new(w, codec::BytesCodec::new());
	sink.send_all(&mut st).await?;
	Ok(())
}
pub async fn read_snapshot_stream<W: AsyncWrite + Unpin>(
	writer: W,
	st: impl Stream<Item = anyhow::Result<Bytes>> + Unpin,
) -> anyhow::Result<()> {
	let st = st.map(|res| res.map_err(|_| std::io::Error::from(std::io::ErrorKind::Other)));
	read_bytes_stream(writer, st).await
}