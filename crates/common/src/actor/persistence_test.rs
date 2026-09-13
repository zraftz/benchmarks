//! Actor ordering tests use a controlled persistence completion and real peer framing.
use super::*;
use crate::apply_worker::ApplyWorker;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use tokio::{net::TcpListener, time::timeout};
static NEXT: AtomicUsize = AtomicUsize::new(0);

struct PendingEngine {
    pending: bool,
    command: Option<Command>,
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
    durable: Arc<AtomicBool>,
    peers: Arc<AtomicUsize>,
    fail: bool,
    omit: bool,
}
impl Engine for PendingEngine {
    type Peer = ();
    type Gate = ();
    fn persistence_pending(&self) -> bool {
        self.pending
    }
    fn complete_persistence(&mut self) -> Result<Option<Vec<Effect>>> {
        self.entered.send(()).unwrap();
        self.release.recv_timeout(Duration::from_secs(5)).unwrap();
        if self.fail {
            bail!("injected local persistence failure")
        }
        if self.omit {
            return Ok(None);
        }
        self.pending = false;
        self.durable.store(true, Ordering::SeqCst);
        Ok(Some(vec![Effect::Apply {
            index: 1,
            command: self.command.take(),
        }]))
    }
    fn decode_peer(&self, _: u64, _: &[u8]) -> Result<()> {
        Ok(())
    }
    fn peer_gate(&self, _: usize, _: usize) {}
    fn admit_peer(_: &mut (), _: &()) -> bool {
        false
    }
    fn peer_batch(&mut self, _: Vec<()>) -> Result<Vec<Effect>> {
        assert!(self.durable.load(Ordering::SeqCst));
        self.peers.fetch_add(1, Ordering::SeqCst);
        Ok(vec![])
    }
    fn leader(&self) -> Option<u64> {
        Some(1)
    }
    fn is_leader(&self) -> bool {
        true
    }
    fn tick(&mut self) -> Result<Vec<Effect>> {
        assert!(!self.pending);
        Ok(vec![])
    }
    fn propose(&mut self, commands: Vec<Command>) -> Result<Vec<Effect>> {
        assert!(!self.pending);
        self.command = commands.into_iter().next();
        self.pending = true;
        Ok(vec![Effect::Send {
            to: 2,
            data: b"eligible append".to_vec(),
        }])
    }
    fn stats(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}
fn command() -> Command {
    Command {
        client: "pipeline".into(),
        sequence: 1,
        kind: "put".into(),
        key: "key".into(),
        value: "durable".into(),
        expected: None,
    }
}
fn state(engine: PendingEngine, outbound: OutboundPeers) -> State<PendingEngine> {
    let path = std::env::temp_dir().join(format!(
        "persistence-owner-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let model = DurableModel::open(&path).unwrap();
    State {
        diagnostics: Diagnostics::default(),
        config: Config {
            id: 1,
            cluster: "test".into(),
            client: String::new(),
            peer: String::new(),
            peers: BTreeMap::new(),
            data_dir: path,
            tick_ms: 20,
            capacity: 16,
            batch_size: 8,
            peer_batch_size: 8,
            diagnostics: false,
            ordered_apply: false,
            peer_message_stream: true,
            pipelined_durability: false,
            max_speculative_proposals: 1,
            max_inflight_appends: 8,
            combine_peer_proposals: false,
            durable_completion_priority: false,
            openraft_async_flush: false,
            snapshot_interval_entries: 0,
        },
        name: "test",
        engine,
        application: application::Application::Inline(model),
        dispatched: 0,
        outbound,
        transport_epoch: (0, None),
        transport_generation: 0,
        dropped: Arc::new(AtomicU64::new(0)),
        pending: BTreeMap::new(),
        pending_count: 0,
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
        snapshot_payload_bytes: 0,
        application_snapshots_installed: 0,
    }
}
fn engine() -> (PendingEngine, mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (entered, waiting) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    (
        PendingEngine {
            pending: false,
            command: None,
            entered,
            release: resume,
            durable: Arc::new(AtomicBool::new(false)),
            peers: Arc::new(AtomicUsize::new(0)),
            fail: false,
            omit: false,
        },
        waiting,
        release,
    )
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eligible_peer_send_overlaps_local_wait_but_consensus_and_client_wait() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let outbound = Outbound::start(
        listener.local_addr().unwrap().to_string(),
        1,
        true,
        Diagnostics::default(),
        Arc::new(AtomicU64::new(0)),
    );
    let (engine, waiting, release) = engine();
    let durable = engine.durable.clone();
    let peers = engine.peers.clone();
    let mut state = state(engine, BTreeMap::from([(2, outbound)]));
    let path = state.config.data_dir.clone();
    let (tx, rx) = mpsc::sync_channel(8);
    let (reply, mut response) = oneshot::channel();
    tx.send(Input::Client(
        Request {
            op: "execute".into(),
            command: Some(command()),
        },
        reply,
        None,
    ))
    .unwrap();
    let owner = std::thread::spawn(move || state.run(rx));
    waiting.recv_timeout(Duration::from_secs(2)).unwrap();
    let (mut stream, _) = timeout(Duration::from_secs(2), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let frame = timeout(Duration::from_secs(2), crate::net::read_frame(&mut stream))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&frame[8..], b"eligible append");
    tx.send(Input::Peer(2, vec![], None)).unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!durable.load(Ordering::SeqCst));
    assert_eq!(peers.load(Ordering::SeqCst), 0);
    assert!(matches!(
        response.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    release.send(()).unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), response)
            .await
            .unwrap()
            .unwrap()
            .status,
        "ok"
    );
    drop(tx);
    owner.join().unwrap().unwrap();
    assert_eq!(peers.load(Ordering::SeqCst), 1);
    let recovered = DurableModel::open(&path).unwrap();
    assert_eq!(recovered.model.values["key"], "durable");
    drop(recovered);
    std::fs::remove_file(path).unwrap();
}
#[test]
fn failed_or_missing_completion_releases_no_application_or_client_success() {
    for missing in [false, true] {
        let (mut engine, _, release) = engine();
        // Keep the observer alive so the test controls the whole completion boundary.
        let (entered, _waiting) = mpsc::channel();
        engine.entered = entered;
        engine.pending = true;
        engine.command = Some(command());
        engine.fail = !missing;
        engine.omit = missing;
        let mut state = state(engine, BTreeMap::new());
        let path = state.config.data_dir.clone();
        let (reply, mut response) = oneshot::channel();
        state
            .pending
            .insert(command().identity(), (command(), vec![reply]));
        state.pending_count = 1;
        release.send(()).unwrap();
        assert!(state.complete_persistence().is_err());
        assert!(state.engine.persistence_pending());
        assert_eq!(state.dispatched, 0);
        assert!(matches!(
            response.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        drop(state);
        let recovered = DurableModel::open(&path).unwrap();
        assert_eq!(recovered.index, 0);
        drop(recovered);
        std::fs::remove_file(path).unwrap();
    }
}
#[test]
fn an_apply_effect_cannot_escape_while_local_persistence_is_pending() {
    let (mut engine, _waiting, _release) = engine();
    engine.pending = true;
    let mut state = state(engine, BTreeMap::new());
    let path = state.config.data_dir.clone();
    assert!(state
        .effects(vec![Effect::Apply {
            index: 1,
            command: Some(command())
        }])
        .is_err());
    assert_eq!(state.dispatched, 0);
    drop(state);
    let recovered = DurableModel::open(&path).unwrap();
    assert_eq!(recovered.index, 0);
    drop(recovered);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn completion_priority_releases_older_durable_application_before_newer_raft_wait() {
    let application_path = std::env::temp_dir().join(format!(
        "priority-application-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let worker = ApplyWorker::start(
        DurableModel::open(&application_path).unwrap(),
        Diagnostics::default(),
        || {},
    );
    let old = command();
    worker
        .try_submit(vec![Applied {
            index: 1,
            command: Some(old.clone()),
            metadata: None,
        }])
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while worker.applied_index() != 1 {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }

    let (mut engine, entered, release) = engine();
    engine.pending = true;
    let mut state = state(engine, BTreeMap::new());
    let owner_path = state.config.data_dir.clone();
    state.config.ordered_apply = true;
    state.config.pipelined_durability = true;
    state.config.durable_completion_priority = true;
    state.application = application::Application::Worker(worker);
    let (reply, mut response) = oneshot::channel();
    state.pending.insert(old.identity(), (old, vec![reply]));
    state.pending_count = 1;
    let (tx, rx) = mpsc::sync_channel(1);
    tx.send(Input::Wake).unwrap();
    let owner = std::thread::spawn(move || {
        let result = state.run(rx);
        (state, result)
    });

    entered.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(response.try_recv().unwrap().status, "ok");
    release.send(()).unwrap();
    drop(tx);
    let (state, result) = owner.join().unwrap();
    result.unwrap();
    assert_eq!(state.pre_persistence_client_completions, 1);
    drop(state);
    std::fs::remove_file(owner_path).unwrap();
    std::fs::remove_file(application_path).unwrap();
}
