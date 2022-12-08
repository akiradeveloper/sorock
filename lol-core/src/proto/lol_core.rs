#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyReq {
    #[prost(bytes = "vec", tag = "1")]
    pub message: ::prost::alloc::vec::Vec<u8>,
    #[prost(bool, tag = "2")]
    pub mutation: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitReq {
    #[prost(bytes = "vec", tag = "1")]
    pub message: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LowLevelApplyReq {
    #[prost(bool, tag = "1")]
    pub core: bool,
    #[prost(bytes = "vec", tag = "2")]
    pub message: ::prost::alloc::vec::Vec<u8>,
    #[prost(bool, tag = "3")]
    pub mutation: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LowLevelCommitReq {
    #[prost(bool, tag = "1")]
    pub core: bool,
    #[prost(bytes = "vec", tag = "2")]
    pub message: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyRep {
    #[prost(bytes = "vec", tag = "1")]
    pub message: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitRep {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendStreamHeader {
    #[prost(string, tag = "1")]
    pub sender_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub prev_log_term: u64,
    #[prost(uint64, tag = "3")]
    pub prev_log_index: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendStreamEntry {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, tag = "2")]
    pub index: u64,
    #[prost(bytes = "bytes", tag = "3")]
    pub command: ::prost::bytes::Bytes,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendEntryReq {
    #[prost(oneof = "append_entry_req::Elem", tags = "1, 2")]
    pub elem: ::core::option::Option<append_entry_req::Elem>,
}
/// Nested message and enum types in `AppendEntryReq`.
pub mod append_entry_req {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Elem {
        #[prost(message, tag = "1")]
        Header(super::AppendStreamHeader),
        #[prost(message, tag = "2")]
        Entry(super::AppendStreamEntry),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendEntryRep {
    #[prost(bool, tag = "1")]
    pub success: bool,
    #[prost(uint64, tag = "2")]
    pub last_log_index: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSnapshotReq {
    #[prost(uint64, tag = "1")]
    pub index: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSnapshotRep {
    #[prost(bytes = "bytes", tag = "1")]
    pub chunk: ::prost::bytes::Bytes,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequestVoteReq {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(string, tag = "2")]
    pub candidate_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub last_log_term: u64,
    #[prost(uint64, tag = "4")]
    pub last_log_index: u64,
    #[prost(bool, tag = "5")]
    pub force_vote: bool,
    #[prost(bool, tag = "6")]
    pub pre_vote: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequestVoteRep {
    #[prost(bool, tag = "1")]
    pub vote_granted: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeartbeatReq {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(string, tag = "2")]
    pub leader_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub leader_commit: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeartbeatRep {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimeoutNowReq {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimeoutNowRep {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddServerReq {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddServerRep {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveServerReq {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveServerRep {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TuneConfigReq {
    #[prost(uint64, optional, tag = "1")]
    pub compaction_interval_sec: ::core::option::Option<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TuneConfigRep {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetConfigReq {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetConfigRep {
    #[prost(uint64, tag = "1")]
    pub compaction_interval_sec: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClusterInfoReq {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClusterInfoRep {
    #[prost(string, optional, tag = "1")]
    pub leader_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "2")]
    pub membership: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatusReq {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatusRep {
    #[prost(uint64, tag = "1")]
    pub snapshot_index: u64,
    #[prost(uint64, tag = "2")]
    pub last_applied: u64,
    #[prost(uint64, tag = "3")]
    pub commit_index: u64,
    #[prost(uint64, tag = "4")]
    pub last_log_index: u64,
}
/// Generated client implementations.
pub mod raft_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct RaftClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl RaftClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> RaftClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> RaftClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            RaftClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        pub async fn request_vote(
            &mut self,
            request: impl tonic::IntoRequest<super::RequestVoteReq>,
        ) -> Result<tonic::Response<super::RequestVoteRep>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/lol_core.Raft/RequestVote",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn send_append_entry(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::AppendEntryReq>,
        ) -> Result<tonic::Response<super::AppendEntryRep>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/lol_core.Raft/SendAppendEntry",
            );
            self.inner
                .client_streaming(request.into_streaming_request(), path, codec)
                .await
        }
        pub async fn get_snapshot(
            &mut self,
            request: impl tonic::IntoRequest<super::GetSnapshotReq>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::GetSnapshotRep>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/lol_core.Raft/GetSnapshot",
            );
            self.inner.server_streaming(request.into_request(), path, codec).await
        }
        pub async fn request_apply(
            &mut self,
            request: impl tonic::IntoRequest<super::ApplyReq>,
        ) -> Result<tonic::Response<super::ApplyRep>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/lol_core.Raft/RequestApply",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn request_commit(
            &mut self,
            request: impl tonic::IntoRequest<super::CommitReq>,
        ) -> Result<tonic::Response<super::CommitRep>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/lol_core.Raft/RequestCommit",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn low_level_request_apply(
            &mut self,
            request: impl tonic::IntoRequest<super::LowLevelApplyReq>,
        ) -> Result<tonic::Response<super::ApplyRep>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/lol_core.Raft/LowLevelRequestApply",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn low_level_request_commit(
            &mut self,
            request: impl tonic::IntoRequest<super::LowLevelCommitReq>,
        ) -> Result<tonic::Response<super::CommitRep>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/lol_core.Raft/LowLevelRequestCommit",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn send_heartbeat(
            &mut self,
            request: impl tonic::IntoRequest<super::HeartbeatReq>,
        ) -> Result<tonic::Response<super::HeartbeatRep>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/lol_core.Raft/SendHeartbeat",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn timeout_now(
            &mut self,
            request: impl tonic::IntoRequest<super::TimeoutNowReq>,
        ) -> Result<tonic::Response<super::TimeoutNowRep>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/lol_core.Raft/TimeoutNow");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn add_server(
            &mut self,
            request: impl tonic::IntoRequest<super::AddServerReq>,
        ) -> Result<tonic::Response<super::AddServerRep>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/lol_core.Raft/AddServer");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn remove_server(
            &mut self,
            request: impl tonic::IntoRequest<super::RemoveServerReq>,
        ) -> Result<tonic::Response<super::RemoveServerRep>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/lol_core.Raft/RemoveServer",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn tune_config(
            &mut self,
            request: impl tonic::IntoRequest<super::TuneConfigReq>,
        ) -> Result<tonic::Response<super::TuneConfigRep>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/lol_core.Raft/TuneConfig");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn get_config(
            &mut self,
            request: impl tonic::IntoRequest<super::GetConfigReq>,
        ) -> Result<tonic::Response<super::GetConfigRep>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/lol_core.Raft/GetConfig");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn request_cluster_info(
            &mut self,
            request: impl tonic::IntoRequest<super::ClusterInfoReq>,
        ) -> Result<tonic::Response<super::ClusterInfoRep>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/lol_core.Raft/RequestClusterInfo",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn status(
            &mut self,
            request: impl tonic::IntoRequest<super::StatusReq>,
        ) -> Result<tonic::Response<super::StatusRep>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/lol_core.Raft/Status");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod raft_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with RaftServer.
    #[async_trait]
    pub trait Raft: Send + Sync + 'static {
        async fn request_vote(
            &self,
            request: tonic::Request<super::RequestVoteReq>,
        ) -> Result<tonic::Response<super::RequestVoteRep>, tonic::Status>;
        async fn send_append_entry(
            &self,
            request: tonic::Request<tonic::Streaming<super::AppendEntryReq>>,
        ) -> Result<tonic::Response<super::AppendEntryRep>, tonic::Status>;
        /// Server streaming response type for the GetSnapshot method.
        type GetSnapshotStream: futures_core::Stream<
                Item = Result<super::GetSnapshotRep, tonic::Status>,
            >
            + Send
            + 'static;
        async fn get_snapshot(
            &self,
            request: tonic::Request<super::GetSnapshotReq>,
        ) -> Result<tonic::Response<Self::GetSnapshotStream>, tonic::Status>;
        async fn request_apply(
            &self,
            request: tonic::Request<super::ApplyReq>,
        ) -> Result<tonic::Response<super::ApplyRep>, tonic::Status>;
        async fn request_commit(
            &self,
            request: tonic::Request<super::CommitReq>,
        ) -> Result<tonic::Response<super::CommitRep>, tonic::Status>;
        async fn low_level_request_apply(
            &self,
            request: tonic::Request<super::LowLevelApplyReq>,
        ) -> Result<tonic::Response<super::ApplyRep>, tonic::Status>;
        async fn low_level_request_commit(
            &self,
            request: tonic::Request<super::LowLevelCommitReq>,
        ) -> Result<tonic::Response<super::CommitRep>, tonic::Status>;
        async fn send_heartbeat(
            &self,
            request: tonic::Request<super::HeartbeatReq>,
        ) -> Result<tonic::Response<super::HeartbeatRep>, tonic::Status>;
        async fn timeout_now(
            &self,
            request: tonic::Request<super::TimeoutNowReq>,
        ) -> Result<tonic::Response<super::TimeoutNowRep>, tonic::Status>;
        async fn add_server(
            &self,
            request: tonic::Request<super::AddServerReq>,
        ) -> Result<tonic::Response<super::AddServerRep>, tonic::Status>;
        async fn remove_server(
            &self,
            request: tonic::Request<super::RemoveServerReq>,
        ) -> Result<tonic::Response<super::RemoveServerRep>, tonic::Status>;
        async fn tune_config(
            &self,
            request: tonic::Request<super::TuneConfigReq>,
        ) -> Result<tonic::Response<super::TuneConfigRep>, tonic::Status>;
        async fn get_config(
            &self,
            request: tonic::Request<super::GetConfigReq>,
        ) -> Result<tonic::Response<super::GetConfigRep>, tonic::Status>;
        async fn request_cluster_info(
            &self,
            request: tonic::Request<super::ClusterInfoReq>,
        ) -> Result<tonic::Response<super::ClusterInfoRep>, tonic::Status>;
        async fn status(
            &self,
            request: tonic::Request<super::StatusReq>,
        ) -> Result<tonic::Response<super::StatusRep>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct RaftServer<T: Raft> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Raft> RaftServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for RaftServer<T>
    where
        T: Raft,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/lol_core.Raft/RequestVote" => {
                    #[allow(non_camel_case_types)]
                    struct RequestVoteSvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::RequestVoteReq>
                    for RequestVoteSvc<T> {
                        type Response = super::RequestVoteRep;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RequestVoteReq>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).request_vote(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RequestVoteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/lol_core.Raft/SendAppendEntry" => {
                    #[allow(non_camel_case_types)]
                    struct SendAppendEntrySvc<T: Raft>(pub Arc<T>);
                    impl<
                        T: Raft,
                    > tonic::server::ClientStreamingService<super::AppendEntryReq>
                    for SendAppendEntrySvc<T> {
                        type Response = super::AppendEntryRep;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::AppendEntryReq>,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).send_append_entry(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SendAppendEntrySvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.client_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/lol_core.Raft/GetSnapshot" => {
                    #[allow(non_camel_case_types)]
                    struct GetSnapshotSvc<T: Raft>(pub Arc<T>);
                    impl<
                        T: Raft,
                    > tonic::server::ServerStreamingService<super::GetSnapshotReq>
                    for GetSnapshotSvc<T> {
                        type Response = super::GetSnapshotRep;
                        type ResponseStream = T::GetSnapshotStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetSnapshotReq>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).get_snapshot(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetSnapshotSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/lol_core.Raft/RequestApply" => {
                    #[allow(non_camel_case_types)]
                    struct RequestApplySvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::ApplyReq>
                    for RequestApplySvc<T> {
                        type Response = super::ApplyRep;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ApplyReq>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).request_apply(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RequestApplySvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/lol_core.Raft/RequestCommit" => {
                    #[allow(non_camel_case_types)]
                    struct RequestCommitSvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::CommitReq>
                    for RequestCommitSvc<T> {
                        type Response = super::CommitRep;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CommitReq>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).request_commit(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RequestCommitSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/lol_core.Raft/LowLevelRequestApply" => {
                    #[allow(non_camel_case_types)]
                    struct LowLevelRequestApplySvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::LowLevelApplyReq>
                    for LowLevelRequestApplySvc<T> {
                        type Response = super::ApplyRep;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LowLevelApplyReq>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).low_level_request_apply(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = LowLevelRequestApplySvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/lol_core.Raft/LowLevelRequestCommit" => {
                    #[allow(non_camel_case_types)]
                    struct LowLevelRequestCommitSvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::LowLevelCommitReq>
                    for LowLevelRequestCommitSvc<T> {
                        type Response = super::CommitRep;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LowLevelCommitReq>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).low_level_request_commit(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = LowLevelRequestCommitSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/lol_core.Raft/SendHeartbeat" => {
                    #[allow(non_camel_case_types)]
                    struct SendHeartbeatSvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::HeartbeatReq>
                    for SendHeartbeatSvc<T> {
                        type Response = super::HeartbeatRep;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::HeartbeatReq>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).send_heartbeat(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SendHeartbeatSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/lol_core.Raft/TimeoutNow" => {
                    #[allow(non_camel_case_types)]
                    struct TimeoutNowSvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::TimeoutNowReq>
                    for TimeoutNowSvc<T> {
                        type Response = super::TimeoutNowRep;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::TimeoutNowReq>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).timeout_now(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = TimeoutNowSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/lol_core.Raft/AddServer" => {
                    #[allow(non_camel_case_types)]
                    struct AddServerSvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::AddServerReq>
                    for AddServerSvc<T> {
                        type Response = super::AddServerRep;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AddServerReq>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).add_server(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = AddServerSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/lol_core.Raft/RemoveServer" => {
                    #[allow(non_camel_case_types)]
                    struct RemoveServerSvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::RemoveServerReq>
                    for RemoveServerSvc<T> {
                        type Response = super::RemoveServerRep;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RemoveServerReq>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).remove_server(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RemoveServerSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/lol_core.Raft/TuneConfig" => {
                    #[allow(non_camel_case_types)]
                    struct TuneConfigSvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::TuneConfigReq>
                    for TuneConfigSvc<T> {
                        type Response = super::TuneConfigRep;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::TuneConfigReq>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).tune_config(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = TuneConfigSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/lol_core.Raft/GetConfig" => {
                    #[allow(non_camel_case_types)]
                    struct GetConfigSvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::GetConfigReq>
                    for GetConfigSvc<T> {
                        type Response = super::GetConfigRep;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetConfigReq>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_config(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetConfigSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/lol_core.Raft/RequestClusterInfo" => {
                    #[allow(non_camel_case_types)]
                    struct RequestClusterInfoSvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::ClusterInfoReq>
                    for RequestClusterInfoSvc<T> {
                        type Response = super::ClusterInfoRep;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ClusterInfoReq>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).request_cluster_info(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RequestClusterInfoSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/lol_core.Raft/Status" => {
                    #[allow(non_camel_case_types)]
                    struct StatusSvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::StatusReq>
                    for StatusSvc<T> {
                        type Response = super::StatusRep;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StatusReq>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).status(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = StatusSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: Raft> Clone for RaftServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Raft> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Raft> tonic::server::NamedService for RaftServer<T> {
        const NAME: &'static str = "lol_core.Raft";
    }
}
