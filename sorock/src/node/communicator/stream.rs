use super::*;

pub fn into_external_replication_stream(
    shard_index: ShardIndex,
    st: request::ReplicationStream,
) -> impl futures::stream::Stream<Item = raft::ReplicationStreamChunk> {
    use raft::replication_stream_chunk::Elem as ChunkElem;
    let header_stream = vec![Some(ChunkElem::Header(raft::ReplicationStreamHeader {
        shard_index,
        sender_id: st.sender_id.to_string(),
        sender_term: st.sender_term,
        prev_clock: Some(raft::Clock {
            term: st.prev_clock.term,
            index: st.prev_clock.index,
        }),
    }))];
    let header_stream = futures::stream::iter(header_stream);

    let chunk_stream = st.entries.map(|e0| {
        e0.map(|e| {
            ChunkElem::Entry(raft::ReplicationStreamEntry {
                clock: Some(raft::Clock {
                    term: e.this_clock.term,
                    index: e.this_clock.index,
                }),
                command: e.command,
            })
        })
    });

    header_stream
        .chain(chunk_stream)
        .map(|elem| raft::ReplicationStreamChunk { elem })
}

pub fn into_internal_snapshot_stream(
    out_stream: impl Stream<Item = Result<raft::SnapshotChunk, tonic::Status>>,
) -> impl Stream<Item = Result<Bytes>> {
    out_stream.map(|result| {
        result
            .map(|chunk| chunk.data.into())
            .map_err(|e| Error::BadSnapshotChunk(e).into())
    })
}
