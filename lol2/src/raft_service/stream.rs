use super::*;

pub async fn into_internal_log_stream(
    mut out_stream: tonic::Streaming<raft::LogStreamChunk>,
) -> (LaneId, LogStream) {
    use raft::log_stream_chunk::Elem as ChunkElem;

    let (lane_id, sender_id, prev_clock) = if let Some(Ok(chunk)) = out_stream.next().await {
        let e = chunk.elem.unwrap();
        if let ChunkElem::Header(raft::LogStreamHeader {
            lane_id,
            sender_id,
            prev_clock: Some(prev_clock),
        }) = e
        {
            (
                lane_id,
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

    let st = LogStream {
        sender_id: sender_id.parse().unwrap(),
        prev_clock,
        entries: Box::pin(entries),
    };

    (lane_id, st)
}

pub type SnapshotStreamOut = std::pin::Pin<
    Box<dyn futures::stream::Stream<Item = Result<raft::SnapshotChunk, tonic::Status>> + Send>,
>;

pub fn into_external_snapshot_stream(in_stream: snapshot::SnapshotStream) -> SnapshotStreamOut {
    let out_stream = in_stream.map(|res| {
        res.map(|data| raft::SnapshotChunk { data })
            .map_err(|_| tonic::Status::unknown("streaming error"))
    });
    Box::pin(out_stream)
}
