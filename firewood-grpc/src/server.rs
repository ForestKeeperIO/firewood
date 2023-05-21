use tonic::{transport::Server, Status};

pub mod sync_server {
    tonic::include_proto!("sync");
}
use sync_server::{ChangeProofRequest, ChangeProofResponse};
use sync_server::{RangeProofRequest, RangeProofResponse};

use sync_server::syncer_server::{Syncer, SyncerServer};

// defining a struct for our service
#[derive(Default)]
pub struct SyncService {}

// implementing rpc for service defined in .proto
#[tonic::async_trait]
impl Syncer for SyncService {
    async fn range_proof(&self, _req: tonic::Request<RangeProofRequest>) -> Result<tonic::Response<RangeProofResponse>, Status> {
        todo!()
    }
    async fn change_proof(&self, _req: tonic::Request<ChangeProofRequest>) -> Result<tonic::Response<ChangeProofResponse>, Status> {
        unimplemented!()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
// defining address for our service
    let addr = "[::1]:50051".parse().unwrap();
// creating a service
    let syncer = SyncService::default();
    println!("Server listening on {}", addr);
// adding our service to our server.
    Server::builder()
        .add_service(SyncerServer::new(syncer))
        .serve(addr)
        .await?;
    Ok(())
}
