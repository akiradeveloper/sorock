use std::task::{Context, Poll};
use tonic::body::BoxBody;
use tonic::transport::Channel;
use tower::Service;

impl Service<http::Request<BoxBody>> for super::Gateway {
    type Response = <Channel as Service<http::Request<BoxBody>>>::Response;
    type Error = <Channel as Service<http::Request<BoxBody>>>::Error;
    type Future = <Channel as Service<http::Request<BoxBody>>>::Future;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Service::poll_ready(&mut self.chan, cx)
    }
    fn call(&mut self, request: http::Request<BoxBody>) -> Self::Future {
        Service::call(&mut self.chan, request)
    }
}
