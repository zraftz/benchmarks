//! Shared single-owner embedding for the two synchronous Raft engines.
//! Durable engine effects precede application fsync and client acknowledgment.
use crate::{
    diagnostics::Diagnostics,
    model::{ApplicationSnapshot, Applied, Command, DurableModel},
    net::{outbound::Outbound, Handler},
    Config, Reply, Request, CONTRACT,
};
use anyhow::{bail, Result};
use async_trait::async_trait;
use std::{
    collections::{btree_map::Entry, BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

pub enum Effect {
    Send {
        to: u64,
        data: Vec<u8>,
    },
    Apply {
        index: u64,
        command: Option<Command>,
    },
    InstallSnapshot {
        snapshot: ApplicationSnapshot,
    },
}
pub trait Engine: Send + 'static {
    type Peer;
    type Gate;
    fn start(&mut self, _diagnostics: bool) {}
    /// One prepared local persistence operation owns consensus state until completed.
    /// While pending, term/leader reads must use metadata retained by the owner.
    fn persistence_pending(&self) -> bool {
        false
    }
    /// Waits for that operation's identity-checked completion and returns fenced effects.
    /// Engines release only eligible replication sends before this method succeeds.
    fn complete_persistence(&mut self) -> Result<Option<Vec<Effect>>> {
        Ok(None)
    }
    fn term(&self) -> u64 {
        0
    }
    fn applied(&mut self, _index: u64) {}
    fn committed_through(&self) -> u64 {
        0
    }
    fn last_index(&self) -> u64 {
        0
    }
    fn snapshot_compaction_supported(&self) -> bool {
        false
    }
    fn snapshot_index(&self) -> u64 {
        0
    }
    fn compact_snapshot(&mut self, _applied_index: u64, _payload: Vec<u8>) -> Result<()> {
        bail!("snapshot compaction is unsupported by this engine")
    }
    fn committed_entries(&self, _after: u64, _count: usize, _bytes: usize) -> Result<Vec<Applied>> {
        bail!("ordered application is unsupported by this build")
    }
    fn decode_peer(&self, from: u64, data: &[u8]) -> Result<Self::Peer>;
    /// Optional queue-delay metric for one decoded peer input.
    fn peer_queue_metric(_peer: &Self::Peer) -> Option<&'static str> {
        None
    }
    fn peer_gate(&self, max_events: usize, max_bytes: usize) -> Self::Gate;
    fn admit_peer(gate: &mut Self::Gate, peer: &Self::Peer) -> bool;
    fn peer_batch(&mut self, peers: Vec<Self::Peer>) -> Result<Vec<Effect>>;
    /// Whether this peer batch can safely share one durable step with ready client proposals.
    fn can_batch_proposals_with_peer(&self, _peers: &[Self::Peer]) -> bool {
        false
    }
    /// Whether this peer batch can safely run before already-collected proposals and a bounded
    /// prefix of ready, unsubmitted execute requests.
    fn can_prioritize_peer_before_proposals(&self, _peers: &[Self::Peer]) -> bool {
        false
    }
    /// Processes a peer batch followed by client proposals in their observed order.
    fn peer_batch_and_propose(
        &mut self,
        _peers: Vec<Self::Peer>,
        _commands: Vec<Command>,
    ) -> Result<Vec<Effect>> {
        bail!("engine declared combined peer/proposal batching without implementing it")
    }
    /// Processes client proposals followed by a peer batch in their observed order.
    fn propose_and_peer_batch(
        &mut self,
        _commands: Vec<Command>,
        _peers: Vec<Self::Peer>,
    ) -> Result<Vec<Effect>> {
        bail!("engine declared combined proposal/peer batching without implementing it")
    }
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
    fn message_oriented(&self) -> bool {
        true
    }
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
            outbound.insert(
                id,
                Outbound::start(
                    address.clone(),
                    config.id,
                    config.peer_message_stream,
                    diagnostics.clone(),
                    dropped.clone(),
                ),
            );
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
                transport_epoch: (0, None),
                transport_generation: 0,
                peer_batches: 0,
                peer_events: 0,
                peer_batch_sizes: BTreeMap::new(),
                prioritized_peer_batches: 0,
                prioritized_peer_events: 0,
                prioritized_client_inputs_bypassed: 0,
                pre_persistence_client_completions: 0,
                snapshot_compactions: 0,
                snapshot_compaction_total_ns: 0,
                snapshot_compaction_max_ns: 0,
                snapshot_compaction_buckets_log2: vec![0; 64],
                snapshot_payload_bytes: 0,
                application_snapshots_installed: 0,
            };
            state.engine.start(state.config.diagnostics);
            let supported = state.config.snapshot_interval_entries == 0
                || state.engine.snapshot_compaction_supported();
            let result = if supported {
                state.effects(recovered).and_then(|_| state.run(rx))
            } else {
                Err(anyhow::anyhow!(
                    "snapshot compaction was configured for an unsupported engine"
                ))
            };
            if let Err(e) = result {
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
mod persistence;

type OutboundPeers = BTreeMap<u64, Outbound>;

type PendingCommands = BTreeMap<(String, u64), (Command, Vec<oneshot::Sender<Reply>>)>;
type ArrivalTimes = Option<Vec<Option<Instant>>>;
type ReadyPeers<P> = (Vec<P>, ArrivalTimes);

struct State<E> {
    diagnostics: Diagnostics,
    config: Config,
    name: &'static str,
    engine: E,
    application: application::Application,
    dispatched: u64,
    outbound: OutboundPeers,
    transport_epoch: (u64, Option<u64>),
    transport_generation: u64,
    dropped: Arc<AtomicU64>,
    pending: PendingCommands,
    pending_count: usize,
    peer_batches: u64,
    peer_events: u64,
    peer_batch_sizes: BTreeMap<usize, u64>,
    prioritized_peer_batches: u64,
    prioritized_peer_events: u64,
    prioritized_client_inputs_bypassed: u64,
    pre_persistence_client_completions: u64,
    snapshot_compactions: u64,
    snapshot_compaction_total_ns: u64,
    snapshot_compaction_max_ns: u64,
    snapshot_compaction_buckets_log2: Vec<u64>,
    snapshot_payload_bytes: u64,
    application_snapshots_installed: u64,
}
impl<E: Engine> State<E> {
    fn run(&mut self, rx: mpsc::Receiver<Input>) -> Result<()> {
        let tick = Duration::from_millis(self.config.tick_ms);
        let mut next = Instant::now() + tick;
        let mut deferred = VecDeque::new();
        loop {
            if self.config.durable_completion_priority && self.engine.persistence_pending() {
                let pending_before = self.pending_count;
                self.complete_applies()?;
                self.pre_persistence_client_completions = self
                    .pre_persistence_client_completions
                    .saturating_add(pending_before.saturating_sub(self.pending_count) as u64);
            }
            self.complete_persistence()?;
            self.complete_applies()?;
            self.maybe_compact_snapshot()?;
            self.dispatch_applies()?;
            if Instant::now() >= next {
                let e = self.engine.tick()?;
                self.effects(e)?;
                next += tick;
                let mut abandoned = Vec::new();
                self.pending.retain(|_, (command, senders)| {
                    senders.retain(|s| !s.is_closed());
                    if senders.is_empty() {
                        abandoned.push(command.clone());
                        false
                    } else {
                        true
                    }
                });
                for command in abandoned {
                    self.diagnostics.abandon_operation(&command);
                }
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
                    let mut arrivals = self.diagnostics.enabled().then(|| vec![queued]);
                    let peers = peer::collect(
                        &self.engine,
                        from,
                        data,
                        self.config.peer_batch_size,
                        &rx,
                        &mut deferred,
                        &self.diagnostics,
                        arrivals.as_mut(),
                    )?;
                    self.peer_batches += 1;
                    self.peer_events += peers.len() as u64;
                    *self.peer_batch_sizes.entry(peers.len()).or_default() += 1;
                    if let Some(arrivals) = &arrivals {
                        for (peer, queued) in peers.iter().zip(arrivals) {
                            if let Some(metric) = E::peer_queue_metric(peer) {
                                self.diagnostics.elapsed(metric, *queued);
                            }
                        }
                    }
                    let mut commands = Vec::new();
                    if self.config.combine_peer_proposals
                        && self.engine.can_batch_proposals_with_peer(&peers)
                    {
                        self.collect_ready_execute_clients(&rx, &mut deferred, &mut commands)?;
                    }
                    let e = if commands.is_empty() {
                        self.engine.peer_batch(peers)?
                    } else {
                        self.diagnostics.proposals_submitted(&commands);
                        self.engine.peer_batch_and_propose(peers, commands)?
                    };
                    if let Some(arrivals) = arrivals {
                        for queued in arrivals {
                            self.diagnostics
                                .elapsed("peer_receive_to_durable_outputs_ns", queued);
                        }
                    }
                    self.effects(e)?;
                }
                Input::Client(request, reply, queued) => {
                    self.diagnostics.elapsed("owner_client_queue_ns", queued);
                    let mut commands = Vec::new();
                    self.client(request, reply, queued, &mut commands)?;
                    self.collect_ready_clients(&rx, &mut deferred, &mut commands, false)?;
                    if !commands.is_empty() {
                        let combined = if self.config.combine_peer_proposals {
                            self.take_ready_combinable_peers(&rx, &mut deferred)?
                        } else {
                            None
                        };
                        let e = if let Some((peers, arrivals)) = combined {
                            self.diagnostics.proposals_submitted(&commands);
                            self.peer_batches = self.peer_batches.saturating_add(1);
                            self.peer_events = self.peer_events.saturating_add(peers.len() as u64);
                            *self.peer_batch_sizes.entry(peers.len()).or_default() += 1;
                            if let Some(arrivals) = &arrivals {
                                for (peer, queued) in peers.iter().zip(arrivals) {
                                    if let Some(metric) = E::peer_queue_metric(peer) {
                                        self.diagnostics.elapsed(metric, *queued);
                                    }
                                }
                            }
                            let outputs = self.engine.propose_and_peer_batch(commands, peers)?;
                            if let Some(arrivals) = arrivals {
                                for queued in arrivals {
                                    self.diagnostics
                                        .elapsed("peer_receive_to_durable_outputs_ns", queued);
                                }
                            }
                            outputs
                        } else {
                            // Keep latency-oriented proposal batches on the speculative path.
                            // Larger batches persist synchronously, so clearing safe replication
                            // acknowledgments first cannot delay a speculative send.
                            if self.config.durable_completion_priority
                                && commands.len() > self.config.max_speculative_proposals
                            {
                                if let Some((peers, arrivals)) =
                                    self.take_ready_prioritizable_peers(&rx, &mut deferred)?
                                {
                                    self.observe_peer_batch(&peers, arrivals.as_deref());
                                    self.prioritized_peer_batches =
                                        self.prioritized_peer_batches.saturating_add(1);
                                    self.prioritized_peer_events = self
                                        .prioritized_peer_events
                                        .saturating_add(peers.len() as u64);
                                    let outputs = self.engine.peer_batch(peers)?;
                                    if let Some(arrivals) = arrivals {
                                        for queued in arrivals {
                                            self.diagnostics.elapsed(
                                                "peer_receive_to_durable_outputs_ns",
                                                queued,
                                            );
                                        }
                                    }
                                    self.effects(outputs)?;
                                }
                            }
                            self.diagnostics.proposals_submitted(&commands);
                            self.engine.propose(commands)?
                        };
                        self.effects(e)?;
                    }
                }
            }
        }
    }
    fn collect_ready_execute_clients(
        &mut self,
        rx: &mpsc::Receiver<Input>,
        deferred: &mut VecDeque<Input>,
        commands: &mut Vec<Command>,
    ) -> Result<()> {
        self.collect_ready_clients(rx, deferred, commands, true)
    }
    fn collect_ready_clients(
        &mut self,
        rx: &mpsc::Receiver<Input>,
        deferred: &mut VecDeque<Input>,
        commands: &mut Vec<Command>,
        execute_only: bool,
    ) -> Result<()> {
        while commands.len() < self.config.batch_size {
            let Some(input) = deferred.pop_front().or_else(|| rx.try_recv().ok()) else {
                break;
            };
            match input {
                Input::Client(q, r, queued) if !execute_only || q.op == "execute" => {
                    self.diagnostics.elapsed("owner_client_queue_ns", queued);
                    self.client(q, r, queued, commands)?;
                }
                other => {
                    deferred.push_front(other);
                    break;
                }
            }
        }
        Ok(())
    }
    fn take_ready_combinable_peers(
        &mut self,
        rx: &mpsc::Receiver<Input>,
        deferred: &mut VecDeque<Input>,
    ) -> Result<Option<ReadyPeers<E::Peer>>> {
        self.take_ready_peers(rx, deferred, false)
    }
    fn take_ready_prioritizable_peers(
        &mut self,
        rx: &mpsc::Receiver<Input>,
        deferred: &mut VecDeque<Input>,
    ) -> Result<Option<ReadyPeers<E::Peer>>> {
        // This is a no-wait lookahead. It restores the original client order at the first
        // non-execute or unsafe consensus boundary and never retains more than one batch.
        self.take_ready_peers(rx, deferred, true)
    }
    fn take_ready_peers(
        &mut self,
        rx: &mpsc::Receiver<Input>,
        deferred: &mut VecDeque<Input>,
        prioritize: bool,
    ) -> Result<Option<ReadyPeers<E::Peer>>> {
        let mut bypassed = Vec::new();
        let (from, data, queued) = loop {
            let Some(input) =
                deferred
                    .pop_front()
                    .or_else(|| if prioritize { rx.try_recv().ok() } else { None })
            else {
                Self::restore_deferred(deferred, bypassed);
                return Ok(None);
            };
            match input {
                Input::Peer(from, data, queued) => break (from, data, queued),
                Input::Client(request, reply, queued)
                    if prioritize
                        && request.op == "execute"
                        && bypassed.len() < self.config.batch_size =>
                {
                    bypassed.push(Input::Client(request, reply, queued));
                }
                boundary => {
                    deferred.push_front(boundary);
                    Self::restore_deferred(deferred, bypassed);
                    return Ok(None);
                }
            }
        };
        let first = self.engine.decode_peer(from, &data)?;
        let eligible = if prioritize {
            self.engine
                .can_prioritize_peer_before_proposals(std::slice::from_ref(&first))
        } else {
            self.engine
                .can_batch_proposals_with_peer(std::slice::from_ref(&first))
        };
        if !eligible {
            deferred.push_front(Input::Peer(from, data, queued));
            Self::restore_deferred(deferred, bypassed);
            return Ok(None);
        }
        self.diagnostics.elapsed("owner_peer_queue_ns", queued);
        let mut gate = self
            .engine
            .peer_gate(self.config.peer_batch_size, 256 * 1024);
        let admitted = E::admit_peer(&mut gate, &first);
        let mut peers = vec![first];
        let mut arrivals = self.diagnostics.enabled().then(|| vec![queued]);
        if admitted {
            while peers.len() < self.config.peer_batch_size {
                let Some(input) = deferred.pop_front().or_else(|| rx.try_recv().ok()) else {
                    break;
                };
                if let Input::Peer(from, data, queued) = &input {
                    let peer = self.engine.decode_peer(*from, data)?;
                    if E::admit_peer(&mut gate, &peer) {
                        peers.push(peer);
                        let eligible = if prioritize {
                            self.engine.can_prioritize_peer_before_proposals(&peers)
                        } else {
                            self.engine.can_batch_proposals_with_peer(&peers)
                        };
                        if eligible {
                            self.diagnostics.elapsed("owner_peer_queue_ns", *queued);
                            if let Some(arrivals) = &mut arrivals {
                                arrivals.push(*queued);
                            }
                            continue;
                        }
                        peers.pop();
                    }
                }
                deferred.push_front(input);
                break;
            }
        }
        self.prioritized_client_inputs_bypassed = self
            .prioritized_client_inputs_bypassed
            .saturating_add(bypassed.len() as u64);
        Self::restore_deferred(deferred, bypassed);
        Ok(Some((peers, arrivals)))
    }
    fn restore_deferred(deferred: &mut VecDeque<Input>, inputs: Vec<Input>) {
        for input in inputs.into_iter().rev() {
            deferred.push_front(input);
        }
    }
    fn observe_peer_batch(&mut self, peers: &[E::Peer], arrivals: Option<&[Option<Instant>]>) {
        self.peer_batches = self.peer_batches.saturating_add(1);
        self.peer_events = self.peer_events.saturating_add(peers.len() as u64);
        *self.peer_batch_sizes.entry(peers.len()).or_default() += 1;
        if let Some(arrivals) = arrivals {
            for (peer, queued) in peers.iter().zip(arrivals) {
                if let Some(metric) = E::peer_queue_metric(peer) {
                    self.diagnostics.elapsed(metric, *queued);
                }
            }
        }
    }
    fn client(
        &mut self,
        q: Request,
        reply: oneshot::Sender<Reply>,
        queued: Option<Instant>,
        commands: &mut Vec<Command>,
    ) -> Result<()> {
        match q.op.as_str() {
            "status" => {
                let info = serde_json::json!({"implementation":self.name,"node_id":self.config.id,
                    "leader":self.engine.is_leader(),"contract":CONTRACT,"application_dispatched_index":self.dispatched,"ordered_apply":self.config.ordered_apply,
                    "engine":self.engine.stats(),"pending":self.pending_count,
                    "diagnostics":self.diagnostics.status_snapshot(),"peer_batches":self.peer_batches,"peer_events":self.peer_events,
                    "peer_batch_sizes":self.peer_batch_sizes,
                    "peer_message_stream":self.config.peer_message_stream,
                    "combine_peer_proposals":self.config.combine_peer_proposals,
                    "durable_completion_priority":{"enabled":self.config.durable_completion_priority,
                        "prioritized_peer_batches":self.prioritized_peer_batches,
                        "prioritized_peer_events":self.prioritized_peer_events,
                        "prioritized_client_inputs_bypassed":self.prioritized_client_inputs_bypassed,
                        "pre_persistence_client_completions":self.pre_persistence_client_completions},
                    "snapshot_compaction":{"interval_entries":self.config.snapshot_interval_entries,
                        "completed":self.snapshot_compactions,
                        "current_index":self.engine.snapshot_index(),
                        "total_ns":self.snapshot_compaction_total_ns,
                        "max_ns":self.snapshot_compaction_max_ns,
                        "buckets_log2":self.snapshot_compaction_buckets_log2,
                        "latest_payload_bytes":self.snapshot_payload_bytes,
                        "application_installs":self.application_snapshots_installed},
                    "transport":self.outbound.iter().map(|(id,peer)|(id.to_string(),peer.stats())).collect::<BTreeMap<_,_>>(),
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
                let identity = c.identity();
                match self.pending.entry(identity) {
                    Entry::Occupied(mut pending) => {
                        if pending.get().0 != c {
                            let _ = reply.send(Reply::applied(crate::model::Outcome {
                                error: Some("identity_conflict".into()),
                                ..Default::default()
                            }));
                            return Ok(());
                        }
                        pending.get_mut().1.push(reply);
                    }
                    Entry::Vacant(pending) => {
                        self.diagnostics.admit_operation(&c, queued);
                        pending.insert((c.clone(), vec![reply]));
                    }
                }
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
        if self.engine.persistence_pending()
            && effects.iter().any(|effect| {
                matches!(
                    effect,
                    Effect::Apply { .. } | Effect::InstallSnapshot { .. }
                )
            })
        {
            bail!("application effect escaped pending Raft persistence")
        }
        let epoch = (self.engine.term(), self.engine.leader());
        if epoch != self.transport_epoch {
            self.transport_epoch = epoch;
            self.transport_generation += 1;
            for peer in self.outbound.values() {
                peer.set_generation(self.transport_generation);
            }
        }
        let mut applies = Vec::new();
        for effect in effects {
            match effect {
                Effect::Send { to, data } => {
                    let Some(tx) = self.outbound.get(&to) else {
                        bail!("outbound peer not in configuration")
                    };
                    if tx.try_send(data, self.diagnostics.start()).is_err() {
                        self.dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Effect::Apply { index, command } => {
                    self.diagnostics
                        .raft_commit_observed(index, "engine_apply_effect");
                    applies.push(Applied {
                        index,
                        command,
                        metadata: None,
                    });
                }
                Effect::InstallSnapshot { snapshot } => {
                    if !applies.is_empty() {
                        bail!("application entries preceded a snapshot install in one Raft output batch")
                    }
                    self.install_application_snapshot(snapshot)?;
                }
            }
        }
        if self.engine.persistence_pending() {
            return Ok(());
        }
        self.apply_effect_entries(applies)
    }

    fn apply_effect_entries(&mut self, applies: Vec<Applied>) -> Result<()> {
        if applies.is_empty() {
            return Ok(());
        }
        match &mut self.application {
            application::Application::Inline(model) => {
                let started = self.diagnostics.start();
                self.diagnostics
                    .observe("application_batch_entries", applies.len() as u64);
                self.diagnostics.application_dispatched(&applies);
                self.diagnostics.application_started(&applies);
                let outcomes = model.apply(&applies)?;
                self.diagnostics.application_durable(&applies);
                if let Some(last) = applies.last() {
                    self.dispatched = last.index;
                    self.engine.applied(last.index);
                }
                let queued = started.map(|started| vec![Some(started); applies.len()]);
                self.resolve_applies(applies, outcomes, queued);
            }
            application::Application::Worker(_) => self.dispatch_applies()?,
        }
        Ok(())
    }
}
