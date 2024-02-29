use super::*;

pub async fn into_internal_log_stream(
    mut out_stream: tonic::Streaming<raft::ReplicationStreamChunk>,
) -> (LaneId, request::ReplicationStream) {
    use raft::replication_stream_chunk::Elem as ChunkElem;

    let (lane_id, sender_id, prev_clock) = if let Some(Ok(chunk)) = out_stream.next().await {
        let e = chunk.elem.expect("replication stream header not found");
        if let ChunkElem::Header(raft::ReplicationStreamHeader {
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
            let e = chunk.elem;
            let e = match e {
                Some(ChunkElem::Entry(raft::ReplicationStreamEntry { clock: Some(clock), command })) => {
                    let e = request::ReplicationStreamElem {
                        this_clock: Clock { term: clock.term, index: clock.index },
                        command: command,
                    };
                    Some(e)
                },
                None => None,
                _ => unreachable!("the replication stream is broken"),
            };
            yield e
        }
    };

    let st = request::ReplicationStream {
        sender_id: sender_id.parse().unwrap(),
        prev_clock,
        entries: Box::pin(entries),
    };

    (lane_id, st)
}

pub type SnapshotStreamOut = std::pin::Pin<
    Box<dyn futures::stream::Stream<Item = Result<raft::SnapshotChunk, tonic::Status>> + Send>,
>;

pub fn into_external_snapshot_stream(in_stream: SnapshotStream) -> SnapshotStreamOut {
    let out_stream = in_stream.map(|res| {
        res.map(|data| raft::SnapshotChunk { data })
            .map_err(|e| tonic::Status::unknown(e.to_string()))
    });
    Box::pin(out_stream)
}
