use anyhow::Result;
use lol_core::{
    simple,
    simple::{RaftAppSimple, ToRaftApp},
};
use std::sync::Arc;

tonic::include_proto!("my_service");

struct MyApp;

struct MyRaftApp {
    my_app: Arc<MyApp>,
}

#[tonic::async_trait]
impl RaftAppSimple for MyRaftApp {
    async fn process_read(&self, request: &[u8]) -> Result<Vec<u8>> {
        println!("raft: process {}", request.len());
        Ok(vec![])
    }
    async fn process_write(&self, _: &[u8]) -> Result<(Vec<u8>, Option<Vec<u8>>)> {
        unimplemented!()
    }
    async fn install_snapshot(&self, _: Option<&[u8]>) -> Result<()> {
        Ok(())
    }
    async fn fold_snapshot(&self, _: Option<&[u8]>, _: Vec<&[u8]>) -> Result<Vec<u8>> {
        Ok(vec![])
    }
}

struct MyServer {
    my_app: Arc<MyApp>,
}
#[tonic::async_trait]
impl my_service_server::MyService for MyServer {
    async fn plus_one(
        &self,
        request: tonic::Request<PlusOneReq>,
    ) -> Result<tonic::Response<PlusOneRep>, tonic::Status> {
        let req = request.into_inner();
        Ok(tonic::Response::new(PlusOneRep { r: req.x + 1 }))
    }
    async fn double(
        &self,
        request: tonic::Request<DoubleReq>,
    ) -> Result<tonic::Response<DoubleRep>, tonic::Status> {
        let req = request.into_inner();
        Ok(tonic::Response::new(DoubleRep { r: req.x * 2 }))
    }
}

async fn run_server() {
    let socket = tokio::net::lookup_host("localhost:50000")
        .await
        .unwrap()
        .next()
        .unwrap();

    let my_app = Arc::new(MyApp);

    // Raft service
    let svc1 = {
        let app = MyRaftApp {
            my_app: Arc::clone(&my_app),
        };
        let store = simple::BytesRepository::new();
        let app = ToRaftApp::new(app, store);
        let storage = lol_core::storage::memory::Storage::new();

        let id = "http://localhost:50000".parse().unwrap();
        let config = lol_core::ConfigBuilder::default().build().unwrap();
        lol_core::make_raft_service(app, storage, id, config).await
    };

    // Non-Raft service
    let svc2 = {
        let svc = MyServer {
            my_app: Arc::clone(&my_app),
        };
        my_service_server::MyServiceServer::new(svc)
    };

    // Register both services `svc1` and `svc2`.
    let mut builder = tonic::transport::Server::builder();
    builder
        .add_service(svc1)
        .add_service(svc2)
        .serve(socket)
        .await
        .expect("server couldn't start well");
}

async fn run_client() {
    let mut cli = {
        let endpoint = tonic::transport::Endpoint::from_static("http://localhost:50000");
        my_service_client::MyServiceClient::connect(endpoint)
            .await
            .unwrap()
    };

    let req1 = DoubleReq { x: 5 };
    let rep1 = cli.double(req1).await.unwrap().into_inner();
    assert_eq!(rep1.r, 10);

    let req2 = PlusOneReq { x: 3 };
    let rep2 = cli.plus_one(req2).await.unwrap().into_inner();
    assert_eq!(rep2.r, 4);
}

#[tokio::main]
async fn main() {
    use std::time::Duration;

    tokio::spawn(run_server());
    tokio::time::sleep(Duration::from_secs(3)).await;
    run_client().await;
}
