//! OpenRaft public API + synced fixture stores + the common framed TCP protocol.
mod network;
mod storage;
use anyhow::Result;
use async_trait::async_trait;
use bench_common::{
    model::{Command, Outcome},
    net::{self, Handler},
    Config, Reply, Request, CONTRACT,
};
use openraft::{BasicNode, Config as RaftConfig, Raft, ServerState, SnapshotPolicy};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

openraft::declare_raft_types!(pub Types: D=Command, R=Outcome, SnapshotData=std::io::Cursor<Vec<u8>>,);
#[derive(Clone)]
struct Service {
    id: u64,
    raft: Raft<Types>,
    logs: storage::LogStore,
    app: storage::StateMachine,
    voters: BTreeMap<u64, BasicNode>,
    admission: Arc<Semaphore>,
}
#[async_trait]
impl Handler for Service {
    async fn client(&self, q: Request) -> Reply {
        match q.op.as_str() {
            "status" => {
                let m = self.raft.metrics().borrow().clone();
                Reply {
                    status: "ok".into(),
                    leader_id: m.current_leader,
                    info: Some(serde_json::json!({
                    "implementation":"openraft","node_id":self.id,"leader":m.state==ServerState::Leader,
                    "contract":CONTRACT,"application":self.app.stats(),"engine":self.logs.stats(),
                    "election_timeout_ms":[1000,2000],"heartbeat_ms":100})),
                    ..Reply::default()
                }
            }
            "dump" => Reply {
                status: "ok".into(),
                info: Some(self.app.dump()),
                ..Reply::default()
            },
            "initialize" => match self.raft.initialize(self.voters.clone()).await {
                Ok(()) => Reply::status("ok"),
                Err(e) => Reply::error(e),
            },
            "execute" => {
                let Some(command) = q.command else {
                    return Reply::error("missing command");
                };
                if let Err(e) = command.validate() {
                    return Reply::error(e);
                }
                let m = self.raft.metrics().borrow().clone();
                if m.state != ServerState::Leader {
                    return Reply {
                        status: "not_leader".into(),
                        leader_id: m.current_leader,
                        ..Reply::default()
                    };
                }
                let Ok(_permit) = self.admission.try_acquire() else {
                    return Reply::status("overloaded");
                };
                match tokio::time::timeout(Duration::from_secs(10), self.raft.client_write(command))
                    .await
                {
                    Ok(Ok(response)) => Reply::applied(response.data),
                    // Once submitted, failure is conservatively unknown. The load generator
                    // retries the identical session/sequence rather than inventing another write.
                    Ok(Err(e)) => Reply {
                        status: "unknown".into(),
                        detail: Some(e.to_string()),
                        ..Reply::default()
                    },
                    Err(_) => Reply::status("unknown"),
                }
            }
            _ => Reply::error("unsupported operation"),
        }
    }
    async fn peer(&self, _from: u64, bytes: Vec<u8>) -> Result<Vec<u8>> {
        match serde_json::from_slice::<network::PeerRequest>(&bytes)? {
            network::PeerRequest::Append(r) => {
                Ok(serde_json::to_vec(&self.raft.append_entries(r).await)?)
            }
            network::PeerRequest::Vote(r) => Ok(serde_json::to_vec(&self.raft.vote(r).await)?),
        }
    }
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let config = Config::load()?;
    let _lock = config.lock_directory("openraft")?;
    let logs = storage::LogStore::open(&config.data_dir.join("raft.wal"))?;
    let app = storage::StateMachine::open(&config.data_dir.join("application.wal"))?;
    app.set_diagnostics(config.diagnostics);
    let cfg = Arc::new(
        RaftConfig {
            cluster_name: config.cluster.clone(),
            election_timeout_min: 1000,
            election_timeout_max: 2000,
            heartbeat_interval: 100,
            snapshot_policy: SnapshotPolicy::Never,
            max_payload_entries: config.batch_size as _,
            ..RaftConfig::default()
        }
        .validate()?,
    );
    let raft = Raft::new(
        config.id,
        cfg,
        network::Network {
            own: config.id,
            peers: config.peers.clone(),
        },
        logs.clone(),
        app.clone(),
    )
    .await?;
    let service = Service {
        id: config.id,
        raft: raft.clone(),
        logs,
        app,
        voters: config
            .peers
            .keys()
            .map(|&id| (id, BasicNode::default()))
            .collect(),
        admission: Arc::new(Semaphore::new(config.capacity)),
    };
    net::serve(config, service).await?;
    raft.shutdown().await?;
    Ok(())
}
