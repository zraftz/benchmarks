use crate::Types;
use bench_common::net::Rpc;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::BTreeMap, io};

#[derive(Serialize, Deserialize)]
pub enum PeerRequest {
    Append(AppendEntriesRequest<Types>),
    Vote(VoteRequest<u64>),
}
#[derive(Clone)]
pub struct Network {
    pub own: u64,
    pub peers: BTreeMap<u64, String>,
}
pub struct Connection {
    rpc: Rpc,
}
impl RaftNetworkFactory<Types> for Network {
    type Network = Connection;
    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Connection {
        Connection {
            rpc: Rpc::new(self.peers[&target].clone(), self.own),
        }
    }
}
impl Connection {
    async fn request<R: DeserializeOwned>(&self, q: &PeerRequest) -> Result<R, io::Error> {
        let bytes = serde_json::to_vec(q).map_err(io::Error::other)?;
        let response = self
            .rpc
            .call(&bytes)
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        serde_json::from_slice(&response).map_err(io::Error::other)
    }
}
impl RaftNetwork<Types> for Connection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<Types>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        // Remote Raft errors are represented as transport failure, never success.
        let r: Result<AppendEntriesResponse<u64>, RaftError<u64>> = self
            .request(&PeerRequest::Append(rpc))
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        r.map_err(|e| RPCError::Network(NetworkError::new(&io::Error::other(e.to_string()))))
    }
    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let r: Result<VoteResponse<u64>, RaftError<u64>> = self
            .request(&PeerRequest::Vote(rpc))
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        r.map_err(|e| RPCError::Network(NetworkError::new(&io::Error::other(e.to_string()))))
    }
    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<Types>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        Err(RPCError::Network(NetworkError::new(&io::Error::other(
            "snapshots not supported in retained-log scenario",
        ))))
    }
}
