use super::*;

pub fn into_external_log_stream(
    lane_id: LaneId,
    st: LogStream,
) -> impl futures::stream::Stream<Item = raft::LogStreamChunk> {
    use raft::log_stream_chunk::Elem as ChunkElem;
    let header_stream = vec![ChunkElem::Header(raft::LogStreamHeader {
        lane_id,
        sender_id: st.sender_id.to_string(),
        prev_clock: Some(raft::Clock {
            term: st.prev_clock.term,
            index: st.prev_clock.index,
        }),
    })];
    let header_stream = futures::stream::iter(header_stream);

    let chunk_stream = st.entries.map(|e| {
        ChunkElem::Entry(raft::LogStreamEntry {
            clock: Some(raft::Clock {
                term: e.this_clock.term,
                index: e.this_clock.index,
            }),
            command: e.command,
        })
    });

    header_stream
        .chain(chunk_stream)
        .map(|e| raft::LogStreamChunk { elem: Some(e) })
}

pub fn into_internal_snapshot_stream(
    out_stream: impl Stream<Item = Result<raft::SnapshotChunk, tonic::Status>>,
) -> impl Stream<Item = Result<Bytes>> {
    out_stream.map(|result| {
        result
            .map(|chunk| chunk.data.into())
            .map_err(|status| anyhow::anyhow!(status.to_string()))
    })
}
