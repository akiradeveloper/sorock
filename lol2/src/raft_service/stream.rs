use super::*;

pub async fn into_internal_log_stream(
    mut out_stream: tonic::Streaming<raft::LogStreamChunk>,
) -> LogStream {
    use raft::log_stream_chunk::Elem as ChunkElem;

    let (sender_id, prev_clock) = if let Some(Ok(chunk)) = out_stream.next().await {
        let e = chunk.elem.unwrap();
        if let ChunkElem::Header(raft::LogStreamHeader {
            sender_id,
            prev_clock: Some(prev_clock),
        }) = e
        {
            (
                sender_id,
                Clock {
                    term: prev_clock.term,
                    index: prev_clock.index,
                },
            )
        } else {
            unreachable!()
        }
    } else {
        unreachable!()
    };

    let entries = async_stream::stream! {
        while let Some(Ok(chunk)) = out_stream.next().await {
            let e = chunk.elem.unwrap();
            match e {
                ChunkElem::Entry(raft::LogStreamEntry { clock: Some(clock), command }) => {
                    let e = LogStreamElem {
                        this_clock: Clock { term: clock.term, index: clock.index },
                        command: command,
                    };
                    yield e;
                },
                _ => unreachable!(),
            }
        }
    };

    LogStream {
        sender_id: sender_id.parse().unwrap(),
        prev_clock,
        entries: Box::pin(entries),
    }
}

pub type SnapshotStreamOut = std::pin::Pin<
    Box<dyn futures::stream::Stream<Item = Result<raft::SnapshotChunk, tonic::Status>> + Send>,
>;

pub fn into_external_snapshot_stream(in_stream: SnapshotStream) -> SnapshotStreamOut {
    let out_stream = in_stream.map(|res| {
        res.map(|data| raft::SnapshotChunk { data })
            .map_err(|_| tonic::Status::unknown("streaming error"))
    });
    Box::pin(out_stream)
}
