//! Shared single-owner embedding for the two synchronous Raft engines.
//! Durable engine effects precede application fsync and client acknowledgment.
use crate::{
    diagnostics::Diagnostics,
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
    type Peer;
    type Gate;
    fn start(&mut self, _diagnostics: bool) {}
    fn applied(&mut self, _index: u64) {}
    fn committed_through(&self) -> u64 {
        0
    }
    fn last_index(&self) -> u64 {
        0
    }
    fn committed_entries(&self, _after: u64, _count: usize, _bytes: usize) -> Result<Vec<Applied>> {
        bail!("ordered application is unsupported by this build")
    }
    fn decode_peer(&self, from: u64, data: &[u8]) -> Result<Self::Peer>;
    fn peer_gate(&self, max_events: usize, max_bytes: usize) -> Self::Gate;
    fn admit_peer(gate: &mut Self::Gate, peer: &Self::Peer) -> bool;
    fn peer_batch(&mut self, peers: Vec<Self::Peer>) -> Result<Vec<Effect>>;
    fn leader(&self) -> Option<u64>;
    fn is_leader(&self) -> bool;
    fn tick(&mut self) -> Result<Vec<Effect>>;
    fn propose(&mut self, commands: Vec<Command>) -> Result<Vec<Effect>>;
    fn stats(&self) -> serde_json::Value;
}
enum Input {
    Client(Request, oneshot::Sender<Reply>, Option<Instant>),
    Peer(u64, Vec<u8>, Option<Instant>),
    Wake,
}
#[derive(Clone)]
pub struct Actor {
    tx: Arc<mpsc::SyncSender<Input>>,
    diagnostics: Diagnostics,
    client_slots: Arc<tokio::sync::Semaphore>,
}
#[async_trait]
impl Handler for Actor {
    async fn client(&self, q: Request) -> Reply {
        let Ok(_slot) = self.client_slots.try_acquire() else {
            return Reply::status("overloaded");
        };
        let (tx, rx) = oneshot::channel();
        match self
            .tx
            .try_send(Input::Client(q, tx, self.diagnostics.start()))
        {
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
            .try_send(Input::Peer(from, data, self.diagnostics.start()))
            .map_err(|_| anyhow::anyhow!("peer inbox unavailable"))?;
        Ok(Vec::new())
    }
}
impl Actor {
    pub fn start<E: Engine>(
        config: Config,
        name: &'static str,
        engine: E,
        mut model: DurableModel,
        recovered: Vec<Effect>,
    ) -> Self {
        let client_limit = config
            .capacity
            .saturating_sub((config.capacity / 4).clamp(1, 64))
            .max(1);
        let client_slots = Arc::new(tokio::sync::Semaphore::new(client_limit));
        let diagnostics = Diagnostics::new(config.diagnostics);
        model.set_diagnostics(diagnostics.clone());
        let (tx, rx) = mpsc::sync_channel(config.capacity);
        let tx = Arc::new(tx);
        let wake = Arc::downgrade(&tx);
        let dropped = Arc::new(AtomicU64::new(0));
        let mut outbound = BTreeMap::new();
        for (&id, address) in &config.peers {
            if id == config.id {
                continue;
            }
            let rpc = Rpc::new(address.clone(), config.id);
            let (send, mut receive) = async_mpsc::channel::<OutboundPacket>(256);
            let lost = dropped.clone();
            let peer_diagnostics = diagnostics.clone();
            tokio::spawn(async move {
                while let Some((data, ready)) = receive.recv().await {
                    peer_diagnostics.elapsed("peer_outbound_queue_ns", ready);
                    if rpc.call(&data).await.is_err() {
                        lost.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
            outbound.insert(id, send);
        }
        let dispatched = model.index;
        let application = if config.ordered_apply {
            application::Application::Worker(crate::apply_worker::ApplyWorker::start(
                model,
                diagnostics.clone(),
                move || {
                    if let Some(tx) = wake.upgrade() {
                        let _ = tx.try_send(Input::Wake);
                    }
                },
            ))
        } else {
            application::Application::Inline(model)
        };
        let owner_diagnostics = diagnostics.clone();
        std::thread::spawn(move || {
            let mut state = State {
                diagnostics: owner_diagnostics,
                config,
                name,
                engine,
                application,
                dispatched,
                outbound,
                dropped,
                pending: BTreeMap::new(),
                pending_count: 0,
                peer_batches: 0,
                peer_events: 0,
                peer_batch_sizes: BTreeMap::new(),
            };
            state.engine.start(state.config.diagnostics);
            if let Err(e) = state.effects(recovered).and_then(|_| state.run(rx)) {
                eprintln!("fatal benchmark node error: {e:#}");
                std::process::exit(1);
            }
        });
        Self {
            tx,
            diagnostics,
            client_slots,
        }
    }
}
mod application;
mod peer;

type OutboundPacket = (Vec<u8>, Option<Instant>);
type OutboundPeers = BTreeMap<u64, async_mpsc::Sender<OutboundPacket>>;

type PendingCommands = BTreeMap<(String, u64), (Command, Vec<oneshot::Sender<Reply>>)>;

struct State<E> {
    diagnostics: Diagnostics,
    config: Config,
    name: &'static str,
    engine: E,
    application: application::Application,
    dispatched: u64,
    outbound: OutboundPeers,
    dropped: Arc<AtomicU64>,
    pending: PendingCommands,
    pending_count: usize,
    peer_batches: u64,
    peer_events: u64,
    peer_batch_sizes: BTreeMap<usize, u64>,
}
impl<E: Engine> State<E> {
    fn run(&mut self, rx: mpsc::Receiver<Input>) -> Result<()> {
        let tick = Duration::from_millis(self.config.tick_ms);
        let mut next = Instant::now() + tick;
        let mut deferred = VecDeque::new();
        loop {
            self.complete_applies()?;
            self.dispatch_applies()?;
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
                Input::Wake => continue,
                Input::Peer(from, data, queued) => {
                    self.diagnostics.elapsed("owner_peer_queue_ns", queued);
                    let mut arrivals = vec![queued];
                    let peers = peer::collect(
                        &self.engine,
                        from,
                        data,
                        self.config.peer_batch_size,
                        &rx,
                        &mut deferred,
                        &self.diagnostics,
                        &mut arrivals,
                    )?;
                    self.peer_batches += 1;
                    self.peer_events += peers.len() as u64;
                    *self.peer_batch_sizes.entry(peers.len()).or_default() += 1;
                    let e = self.engine.peer_batch(peers)?;
                    for queued in arrivals {
                        self.diagnostics
                            .elapsed("peer_receive_to_durable_outputs_ns", queued);
                    }
                    self.effects(e)?;
                }
                Input::Client(request, reply, queued) => {
                    self.diagnostics.elapsed("owner_client_queue_ns", queued);
                    let mut commands = Vec::new();
                    self.client(request, reply, &mut commands)?;
                    while commands.len() < self.config.batch_size {
                        match deferred.pop_front().or_else(|| rx.try_recv().ok()) {
                            Some(Input::Client(q, r, queued)) => {
                                self.diagnostics.elapsed("owner_client_queue_ns", queued);
                                self.client(q, r, &mut commands)?;
                            }
                            Some(i) => {
                                deferred.push_back(i);
                                break;
                            }
                            None => break,
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
                    "leader":self.engine.is_leader(),"contract":CONTRACT,"application_dispatched_index":self.dispatched,"ordered_apply":self.config.ordered_apply,
                    "engine":self.engine.stats(),"pending":self.pending_count,
                    "diagnostics":self.diagnostics.snapshot(),"peer_batches":self.peer_batches,"peer_events":self.peer_events,
                    "peer_batch_sizes":self.peer_batch_sizes,
                    "peer_drops":self.dropped.load(Ordering::Relaxed)});
                self.application.query(crate::apply_worker::Query {
                    reply,
                    dump: false,
                    response: Reply {
                        status: "ok".into(),
                        leader_id: self.engine.leader(),
                        info: Some(info),
                        ..Reply::default()
                    },
                })?;
            }
            "dump" => {
                self.application.query(crate::apply_worker::Query {
                    reply,
                    dump: true,
                    response: Reply::status("ok"),
                })?;
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
                if self.pending_count >= self.config.capacity
                    || self
                        .application
                        .backpressured(self.engine.last_index(), self.dispatched)
                {
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
                    if tx.try_send((data, self.diagnostics.start())).is_err() {
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
        match &mut self.application {
            application::Application::Inline(model) => {
                let started = self.diagnostics.start();
                self.diagnostics
                    .observe("application_batch_entries", applies.len() as u64);
                let outcomes = model.apply(&applies)?;
                if let Some(last) = applies.last() {
                    self.dispatched = last.index;
                    self.engine.applied(last.index);
                }
                let queued = vec![started; applies.len()];
                self.resolve_applies(applies, outcomes, queued);
            }
            application::Application::Worker(_) => self.dispatch_applies()?,
        }
        Ok(())
    }
}
