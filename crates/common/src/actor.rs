//! Shared single-owner embedding for the two synchronous Raft engines.
//! Durable engine effects precede application fsync and client acknowledgment.
use crate::{
    model::{Applied, Command, DurableModel},
    net::{Handler, Rpc},
    Config, Reply, Request, CONTRACT,
};
use anyhow::{bail, Result};
use async_trait::async_trait;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc as async_mpsc, oneshot};

pub enum Effect {
    Send {
        to: u64,
        data: Vec<u8>,
    },
    Apply {
        index: u64,
        command: Option<Command>,
    },
}
pub trait Engine: Send + 'static {
    fn leader(&self) -> Option<u64>;
    fn is_leader(&self) -> bool;
    fn tick(&mut self) -> Result<Vec<Effect>>;
    fn peer(&mut self, from: u64, data: &[u8]) -> Result<Vec<Effect>>;
    fn propose(&mut self, commands: Vec<Command>) -> Result<Vec<Effect>>;
    fn stats(&self) -> serde_json::Value;
}
enum Input {
    Client(Request, oneshot::Sender<Reply>),
    Peer(u64, Vec<u8>),
}
#[derive(Clone)]
pub struct Actor {
    tx: mpsc::SyncSender<Input>,
}
#[async_trait]
impl Handler for Actor {
    async fn client(&self, q: Request) -> Reply {
        let (tx, rx) = oneshot::channel();
        match self.tx.try_send(Input::Client(q, tx)) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Full(_)) => return Reply::status("overloaded"),
            Err(_) => return Reply::error("actor stopped"),
        }
        match tokio::time::timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(r)) => r,
            _ => Reply::status("unknown"),
        }
    }
    async fn peer(&self, from: u64, data: Vec<u8>) -> Result<Vec<u8>> {
        self.tx
            .try_send(Input::Peer(from, data))
            .map_err(|_| anyhow::anyhow!("peer inbox unavailable"))?;
        Ok(Vec::new())
    }
}
impl Actor {
    pub fn start<E: Engine>(
        config: Config,
        name: &'static str,
        engine: E,
        model: DurableModel,
        recovered: Vec<Effect>,
    ) -> Self {
        let (tx, rx) = mpsc::sync_channel(config.capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let mut outbound = BTreeMap::new();
        for (&id, address) in &config.peers {
            if id == config.id {
                continue;
            }
            let rpc = Rpc::new(address.clone(), config.id);
            let (send, mut receive) = async_mpsc::channel::<Vec<u8>>(256);
            let lost = dropped.clone();
            tokio::spawn(async move {
                while let Some(data) = receive.recv().await {
                    if rpc.call(&data).await.is_err() {
                        lost.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
            outbound.insert(id, send);
        }
        std::thread::spawn(move || {
            let mut state = State {
                config,
                name,
                engine,
                model,
                outbound,
                dropped,
                pending: BTreeMap::new(),
                pending_count: 0,
            };
            if let Err(e) = state.effects(recovered).and_then(|_| state.run(rx)) {
                eprintln!("fatal benchmark node error: {e:#}");
                std::process::exit(1);
            }
        });
        Self { tx }
    }
}
type PendingCommands = BTreeMap<(String, u64), (Command, Vec<oneshot::Sender<Reply>>)>;

struct State<E> {
    config: Config,
    name: &'static str,
    engine: E,
    model: DurableModel,
    outbound: BTreeMap<u64, async_mpsc::Sender<Vec<u8>>>,
    dropped: Arc<AtomicU64>,
    pending: PendingCommands,
    pending_count: usize,
}
impl<E: Engine> State<E> {
    fn run(&mut self, rx: mpsc::Receiver<Input>) -> Result<()> {
        let tick = Duration::from_millis(self.config.tick_ms);
        let mut next = Instant::now() + tick;
        let mut deferred = VecDeque::new();
        loop {
            if Instant::now() >= next {
                let e = self.engine.tick()?;
                self.effects(e)?;
                next += tick;
                self.pending.retain(|_, (_, senders)| {
                    senders.retain(|s| !s.is_closed());
                    !senders.is_empty()
                });
                self.pending_count = self.pending.values().map(|(_, v)| v.len()).sum();
                continue;
            }
            let input = if let Some(i) = deferred.pop_front() {
                i
            } else {
                match rx.recv_timeout(next.saturating_duration_since(Instant::now())) {
                    Ok(i) => i,
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(_) => return Ok(()),
                }
            };
            match input {
                Input::Peer(from, data) => {
                    let e = self.engine.peer(from, &data)?;
                    self.effects(e)?;
                }
                Input::Client(request, reply) => {
                    let mut commands = Vec::new();
                    self.client(request, reply, &mut commands)?;
                    while commands.len() < self.config.batch_size {
                        match rx.try_recv() {
                            Ok(Input::Client(q, r)) => self.client(q, r, &mut commands)?,
                            Ok(i) => {
                                deferred.push_back(i);
                                break;
                            }
                            Err(_) => break,
                        }
                    }
                    if !commands.is_empty() {
                        let e = self.engine.propose(commands)?;
                        self.effects(e)?;
                    }
                }
            }
        }
    }
    fn client(
        &mut self,
        q: Request,
        reply: oneshot::Sender<Reply>,
        commands: &mut Vec<Command>,
    ) -> Result<()> {
        match q.op.as_str() {
            "status" => {
                let info = serde_json::json!({"implementation":self.name,"node_id":self.config.id,
                    "leader":self.engine.is_leader(),"contract":CONTRACT,"application":self.model.stats(),
                    "engine":self.engine.stats(),"pending":self.pending_count,
                    "peer_drops":self.dropped.load(Ordering::Relaxed)});
                let _ = reply.send(Reply {
                    status: "ok".into(),
                    leader_id: self.engine.leader(),
                    info: Some(info),
                    ..Reply::default()
                });
            }
            "dump" => {
                let _ = reply.send(Reply {
                    status: "ok".into(),
                    info: Some(serde_json::json!({
                "values":self.model.model.values,"applied_index":self.model.index})),
                    ..Reply::default()
                });
            }
            "execute" => {
                let Some(c) = q.command else {
                    let _ = reply.send(Reply::error("missing command"));
                    return Ok(());
                };
                if let Err(e) = c.validate() {
                    let _ = reply.send(Reply::error(e));
                    return Ok(());
                }
                if !self.engine.is_leader() {
                    let _ = reply.send(Reply {
                        status: "not_leader".into(),
                        leader_id: self.engine.leader(),
                        ..Reply::default()
                    });
                    return Ok(());
                }
                if self.pending_count >= self.config.capacity {
                    let _ = reply.send(Reply::status("overloaded"));
                    return Ok(());
                }
                if let Some((prior, _)) = self.pending.get(&c.identity()) {
                    if prior != &c {
                        let _ = reply.send(Reply::applied(crate::model::Outcome {
                            error: Some("identity_conflict".into()),
                            ..Default::default()
                        }));
                        return Ok(());
                    }
                }
                self.pending
                    .entry(c.identity())
                    .or_insert_with(|| (c.clone(), Vec::new()))
                    .1
                    .push(reply);
                self.pending_count += 1;
                commands.push(c);
            }
            _ => {
                let _ = reply.send(Reply::error("unsupported operation"));
            }
        }
        Ok(())
    }
    fn effects(&mut self, effects: Vec<Effect>) -> Result<()> {
        let mut applies = Vec::new();
        for effect in effects {
            match effect {
                Effect::Send { to, data } => {
                    let Some(tx) = self.outbound.get(&to) else {
                        bail!("outbound peer not in configuration")
                    };
                    if tx.try_send(data).is_err() {
                        self.dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Effect::Apply { index, command } => applies.push(Applied {
                    index,
                    command,
                    metadata: None,
                }),
            }
        }
        let outcomes = self.model.apply(&applies)?;
        for (entry, outcome) in applies.into_iter().zip(outcomes) {
            if let (Some(c), Some(result)) = (entry.command, outcome) {
                if let Some((submitted, replies)) = self.pending.remove(&c.identity()) {
                    self.pending_count -= replies.len();
                    for reply in replies {
                        let response = if submitted == c {
                            result.clone()
                        } else {
                            crate::model::Outcome {
                                error: Some("identity_conflict".into()),
                                ..Default::default()
                            }
                        };
                        let _ = reply.send(Reply::applied(response));
                    }
                }
            }
        }
        Ok(())
    }
}
