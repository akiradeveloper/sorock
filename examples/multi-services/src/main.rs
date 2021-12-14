use lol_core::{
    compat::{RaftAppCompat, ToRaftApp},
    Index, RaftApp,
};
use std::sync::Arc;

tonic::include_proto!("my_service");

struct MyApp;

struct MyRaftApp {
    my_app: Arc<MyApp>,
}

#[tonic::async_trait]
impl RaftAppCompat for MyRaftApp {
    async fn process_message(&self, request: &[u8]) -> anyhow::Result<Vec<u8>> {
        println!("raft: process {}", request.len());
        Ok(vec![])
    }
    async fn apply_message(
        &self,
        request: &[u8],
        apply_index: Index,
    ) -> anyhow::Result<(Vec<u8>, Option<Vec<u8>>)> {
        unimplemented!()
    }
    async fn install_snapshot(
        &self,
        snapshot: Option<&[u8]>,
        apply_index: Index,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    async fn fold_snapshot(
        &self,
        old_snapshot: Option<&[u8]>,
        requests: Vec<&[u8]>,
    ) -> anyhow::Result<Vec<u8>> {
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
        println!("non-raft plus_one {}", req.x);
        Ok(tonic::Response::new(PlusOneRep { r: req.x + 1 }))
    }
    async fn double(
        &self,
        request: tonic::Request<DoubleReq>,
    ) -> Result<tonic::Response<DoubleRep>, tonic::Status> {
        let req = request.into_inner();
        println!("non-raft double {}", req.x);
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
        let app = ToRaftApp::new(app);
        let storage = lol_core::storage::memory::Storage::new();
        let config = lol_core::Config::new("http://localhost:50000".to_owned());
        let mut tunable = lol_core::TunableConfig::default();
        let core = lol_core::RaftCore::new(app, storage, config, tunable).await;
        lol_core::make_service(core)
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
    let mut cli1 = {
        let endpoint = tonic::transport::Endpoint::from_static("http://localhost:50000");
        lol_core::proto_compiled::raft_client::RaftClient::connect(endpoint)
            .await
            .unwrap()
    };
    let mut cli2 = {
        let endpoint = tonic::transport::Endpoint::from_static("http://localhost:50000");
        my_service_client::MyServiceClient::connect(endpoint)
            .await
            .unwrap()
    };

    let req1 = lol_core::proto_compiled::ProcessReq {
        message: vec![0; 100],
        core: false,
    };
    let rep1 = cli1
        .request_process_locally(req1)
        .await
        .unwrap()
        .into_inner();

    let req2 = DoubleReq { x: 5 };
    let rep2 = cli2.double(req2).await.unwrap().into_inner();
    println!("=> {}", rep2.r);

    let req3 = PlusOneReq { x: 3 };
    let rep3 = cli2.plus_one(req3).await.unwrap().into_inner();
    println!("=> {}", rep3.r);

    let req4 = lol_core::proto_compiled::ProcessReq {
        message: vec![0; 200],
        core: false,
    };
    let rep4 = cli1
        .request_process_locally(req4)
        .await
        .unwrap()
        .into_inner();
}

#[tokio::main]
async fn main() {
    use std::time::Duration;

    tokio::spawn(run_server());
    tokio::time::sleep(Duration::from_secs(3)).await;
    run_client().await;
}
