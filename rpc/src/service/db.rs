// Copyright (C) 2023, Ava Labs, Inc. All rights reserved.
// See the file LICENSE.md for licensing terms.

use super::{Database, IntoStatusResultExt};
use crate::sync::{
    db_server::Db as DbServerTrait, CommitChangeProofRequest, CommitRangeProofRequest,
    GetChangeProofRequest, GetChangeProofResponse, GetMerkleRootResponse, GetProofRequest,
    GetProofResponse, GetRangeProofRequest, GetRangeProofResponse, MaybeBytes, Proof as SyncProof,
    ProofNode, SerializedPath, VerifyChangeProofRequest, VerifyChangeProofResponse,
};
use firewood::{
    nibbles::Nibbles,
    v2::api::{Db, DbView},
};
use tonic::{async_trait, Request, Response, Status};

#[async_trait]
impl DbServerTrait for Database {
    async fn get_merkle_root(
        &self,
        _request: Request<()>,
    ) -> Result<Response<GetMerkleRootResponse>, Status> {
        let root_hash = self.db.root_hash().await.into_status_result()?.to_vec();

        let response = GetMerkleRootResponse { root_hash };

        Ok(Response::new(response))
    }

    async fn get_proof(
        &self,
        request: Request<GetProofRequest>,
    ) -> Result<Response<GetProofResponse>, Status> {
        let GetProofRequest { key } = request.into_inner();
        let revision = self.revision().await.into_status_result()?;
        let root_hash = revision.root_hash().await.into_status_result()?;
        // TODO: verfiy that we should never return `is_nothing: false` for `MaybeBytes`
        let value = revision.val(&key).await.into_status_result()?;

        let Some(value) = value else {
            return Ok(Response::new(GetProofResponse { proof: None }));
        };

        let proof = revision
            .single_key_proof::<_, Vec<u8>>(&key)
            .await
            .into_status_result()?
            .map(|proof| {
                let nibbles = Nibbles::<0>::new(&key);
                let nibble_length = nibbles.len() as u64;
                let value = nibbles.into_iter().collect();

                let serialized_path = SerializedPath {
                    nibble_length,
                    value,
                };
                let value = serialized_path.value.clone();

                let key = Some(serialized_path);
                let value_or_hash = Some(value.clone()).map(into_maybe_bytes);

                // TODO: key needs to go from u256 -> u32?
                let mut i = 0;

                let children = proof
                    .0
                    .into_iter()
                    .map(|(_key, value)| {
                        let key = i;
                        i += 1;
                        (key, value)
                    })
                    .collect();

                let proof_node = ProofNode {
                    key,
                    value_or_hash,
                    children,
                };

                // TODO: this is wrong too
                vec![proof_node]
            });

        let proof = proof.map(|proof| SyncProof { key, value, proof });

        let response = GetProofResponse { proof };

        Ok(Response::new(response))
    }

    async fn get_change_proof(
        &self,
        request: Request<GetChangeProofRequest>,
    ) -> Result<Response<GetChangeProofResponse>, Status> {
        let GetChangeProofRequest {
            start_root_hash: _,
            end_root_hash: _,
            start_key: _,
            end_key: _,
            key_limit: _,
        } = request.into_inner();

        let _revision = self.revision().await.into_status_result()?;

        todo!()
    }

    async fn verify_change_proof(
        &self,
        request: Request<VerifyChangeProofRequest>,
    ) -> Result<Response<VerifyChangeProofResponse>, Status> {
        let VerifyChangeProofRequest {
            proof: _,
            start_key: _,
            end_key: _,
            expected_root_hash: _,
        } = request.into_inner();

        let _revision = self.revision().await.into_status_result()?;

        todo!()
    }

    async fn commit_change_proof(
        &self,
        request: Request<CommitChangeProofRequest>,
    ) -> Result<Response<()>, Status> {
        let CommitChangeProofRequest { proof: _ } = request.into_inner();

        todo!()
    }

    async fn get_range_proof(
        &self,
        request: Request<GetRangeProofRequest>,
    ) -> Result<Response<GetRangeProofResponse>, Status> {
        let GetRangeProofRequest {
            root_hash: _,
            start_key: _,
            end_key: _,
            key_limit: _,
        } = request.into_inner();

        todo!()
    }

    async fn commit_range_proof(
        &self,
        request: Request<CommitRangeProofRequest>,
    ) -> Result<Response<()>, Status> {
        let CommitRangeProofRequest {
            start_key: _,
            range_proof: _,
        } = request.into_inner();

        todo!()
    }
}

fn into_maybe_bytes(value: Vec<u8>) -> MaybeBytes {
    MaybeBytes {
        value,
        is_nothing: false,
    }
}
