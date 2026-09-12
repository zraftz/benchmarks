//! FIFO and bounded-drain tests use an engine that rejects a sentinel message.
use super::*;
struct TestEngine;
impl Engine for TestEngine {
    type Peer = u8;
    type Gate = ();
    fn decode_peer(&self, _: u64, data: &[u8]) -> Result<u8> {
        Ok(data[0])
    }
    fn peer_gate(&self, _: usize, _: usize) {}
    fn admit_peer(_: &mut (), peer: &u8) -> bool {
        *peer != 0
    }
    fn peer_batch(&mut self, _: Vec<u8>) -> Result<Vec<Effect>> {
        unreachable!()
    }
    fn leader(&self) -> Option<u64> {
        None
    }
    fn is_leader(&self) -> bool {
        false
    }
    fn tick(&mut self) -> Result<Vec<Effect>> {
        unreachable!()
    }
    fn propose(&mut self, _: Vec<Command>) -> Result<Vec<Effect>> {
        unreachable!()
    }
    fn stats(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}
fn peer(n: u8) -> Input {
    Input::Peer(1, vec![n], None)
}
fn take(
    cap: usize,
    first: u8,
    rx: &mpsc::Receiver<Input>,
    deferred: &mut VecDeque<Input>,
) -> Vec<u8> {
    collect(
        &TestEngine,
        1,
        vec![first],
        cap,
        rx,
        deferred,
        &Diagnostics::default(),
        &mut vec![],
    )
    .unwrap()
}
#[test]
fn drain_stops_at_client_without_scanning_past_it() {
    let (tx, rx) = mpsc::sync_channel(8);
    let (reply, _) = oneshot::channel();
    tx.send(peer(2)).unwrap();
    tx.send(Input::Client(
        Request {
            op: "status".into(),
            command: None,
        },
        reply,
        None,
    ))
    .unwrap();
    tx.send(peer(3)).unwrap();
    let mut deferred = VecDeque::new();
    assert_eq!(take(32, 1, &rx, &mut deferred), vec![1, 2]);
    assert!(matches!(deferred.pop_front(), Some(Input::Client(..))));
    assert!(matches!(rx.try_recv(), Ok(Input::Peer(_, data, _)) if data == vec![3]));
}
#[test]
fn rejected_peer_is_preserved_before_the_next_ready_peer() {
    let (tx, rx) = mpsc::sync_channel(8);
    tx.send(peer(0)).unwrap();
    tx.send(peer(3)).unwrap();
    let mut deferred = VecDeque::from([peer(2)]);
    assert_eq!(take(32, 1, &rx, &mut deferred), vec![1, 2]);
    assert!(matches!(deferred.pop_front(), Some(Input::Peer(_, data, _)) if data == vec![0]));
    assert_eq!(take(32, 0, &rx, &mut deferred), vec![0]);
    assert!(matches!(rx.try_recv(), Ok(Input::Peer(_, data, _)) if data == vec![3]));
}
#[test]
fn cap_leaves_work_queued_and_empty_queue_does_not_wait() {
    let (tx, rx) = mpsc::sync_channel(8);
    tx.send(peer(2)).unwrap();
    tx.send(peer(3)).unwrap();
    let mut deferred = VecDeque::new();
    assert_eq!(take(2, 1, &rx, &mut deferred), vec![1, 2]);
    assert!(matches!(rx.try_recv(), Ok(Input::Peer(_, data, _)) if data == vec![3]));
    assert_eq!(take(32, 4, &rx, &mut deferred), vec![4]);
}

struct CombiningEngine {
    combined: mpsc::Sender<(bool, Vec<u8>, Vec<Command>)>,
}
impl Engine for CombiningEngine {
    type Peer = u8;
    type Gate = ();
    fn decode_peer(&self, _: u64, data: &[u8]) -> Result<u8> {
        Ok(data[0])
    }
    fn peer_gate(&self, _: usize, _: usize) {}
    fn admit_peer(_: &mut (), _: &u8) -> bool {
        true
    }
    fn peer_batch(&mut self, _: Vec<u8>) -> Result<Vec<Effect>> {
        unreachable!("eligible ready proposals should share the peer step")
    }
    fn can_batch_proposals_with_peer(&self, _: &[u8]) -> bool {
        true
    }
    fn peer_batch_and_propose(
        &mut self,
        peers: Vec<u8>,
        commands: Vec<Command>,
    ) -> Result<Vec<Effect>> {
        self.combined.send((true, peers, commands)).unwrap();
        Ok(Vec::new())
    }
    fn propose_and_peer_batch(
        &mut self,
        commands: Vec<Command>,
        peers: Vec<u8>,
    ) -> Result<Vec<Effect>> {
        self.combined.send((false, peers, commands)).unwrap();
        Ok(Vec::new())
    }
    fn leader(&self) -> Option<u64> {
        Some(1)
    }
    fn is_leader(&self) -> bool {
        true
    }
    fn tick(&mut self) -> Result<Vec<Effect>> {
        Ok(Vec::new())
    }
    fn propose(&mut self, _: Vec<Command>) -> Result<Vec<Effect>> {
        unreachable!("eligible ready proposals should share the peer step")
    }
    fn stats(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}

fn combining_state(
    path: &std::path::Path,
    combined: mpsc::Sender<(bool, Vec<u8>, Vec<Command>)>,
) -> State<CombiningEngine> {
    let model = DurableModel::open(path).unwrap();
    State {
        diagnostics: Diagnostics::default(),
        config: Config {
            id: 1,
            cluster: "test".into(),
            client: String::new(),
            peer: String::new(),
            peers: BTreeMap::new(),
            data_dir: path.to_owned(),
            tick_ms: 20,
            capacity: 16,
            batch_size: 8,
            peer_batch_size: 8,
            diagnostics: false,
            ordered_apply: false,
            peer_message_stream: true,
            pipelined_durability: true,
            max_speculative_proposals: 1,
            max_inflight_appends: 8,
            combine_peer_proposals: true,
            durable_completion_priority: false,
            openraft_async_flush: false,
        },
        name: "test",
        engine: CombiningEngine { combined },
        application: application::Application::Inline(model),
        dispatched: 0,
        outbound: BTreeMap::new(),
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
    }
}

fn write(key: &str) -> Input {
    sequenced_write(key, 1)
}

fn sequenced_write(key: &str, sequence: u64) -> Input {
    let (reply, _response) = oneshot::channel();
    Input::Client(
        Request {
            op: "execute".into(),
            command: Some(Command {
                client: "client".into(),
                sequence,
                kind: "put".into(),
                key: key.into(),
                value: "value".into(),
                expected: None,
            }),
        },
        reply,
        None,
    )
}

#[test]
fn same_turn_peer_ack_and_ready_write_use_one_engine_step() {
    let path = std::env::temp_dir().join(format!("mixed-peer-proposal-{}", std::process::id()));
    let (combined, observed) = mpsc::channel();
    let mut state = combining_state(&path, combined);
    let (tx, rx) = mpsc::sync_channel(8);
    tx.send(peer(7)).unwrap();
    tx.send(write("key")).unwrap();
    drop(tx);

    state.run(rx).unwrap();
    let (peer_first, peers, commands) = observed.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(peer_first);
    assert_eq!(peers, vec![7]);
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].key, "key");
    drop(state);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn same_turn_ready_write_and_following_ack_preserve_order_in_one_engine_step() {
    let path = std::env::temp_dir().join(format!("mixed-proposal-peer-{}", std::process::id()));
    let (combined, observed) = mpsc::channel();
    let mut state = combining_state(&path, combined);
    let (tx, rx) = mpsc::sync_channel(8);
    tx.send(write("key")).unwrap();
    tx.send(peer(7)).unwrap();
    tx.send(peer(8)).unwrap();
    drop(tx);

    state.run(rx).unwrap();
    let (peer_first, peers, commands) = observed.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(!peer_first);
    assert_eq!(peers, vec![7, 8]);
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].key, "key");
    drop(state);
    std::fs::remove_file(path).unwrap();
}

struct PriorityEngine {
    observed: mpsc::Sender<&'static str>,
}
impl Engine for PriorityEngine {
    type Peer = u8;
    type Gate = ();
    fn decode_peer(&self, _: u64, data: &[u8]) -> Result<u8> {
        Ok(data[0])
    }
    fn peer_gate(&self, _: usize, _: usize) {}
    fn admit_peer(_: &mut (), _: &u8) -> bool {
        true
    }
    fn peer_batch(&mut self, _: Vec<u8>) -> Result<Vec<Effect>> {
        self.observed.send("peer").unwrap();
        Ok(Vec::new())
    }
    fn can_prioritize_peer_before_proposals(&self, peers: &[u8]) -> bool {
        peers.iter().all(|peer| *peer != 0)
    }
    fn leader(&self) -> Option<u64> {
        Some(1)
    }
    fn is_leader(&self) -> bool {
        true
    }
    fn tick(&mut self) -> Result<Vec<Effect>> {
        Ok(Vec::new())
    }
    fn propose(&mut self, _: Vec<Command>) -> Result<Vec<Effect>> {
        self.observed.send("proposal").unwrap();
        Ok(Vec::new())
    }
    fn stats(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}

fn priority_state(
    path: &std::path::Path,
    observed: mpsc::Sender<&'static str>,
    batch_size: usize,
) -> State<PriorityEngine> {
    let model = DurableModel::open(path).unwrap();
    State {
        diagnostics: Diagnostics::default(),
        config: Config {
            id: 1,
            cluster: "test".into(),
            client: String::new(),
            peer: String::new(),
            peers: BTreeMap::new(),
            data_dir: path.to_owned(),
            tick_ms: 20,
            capacity: 16,
            batch_size,
            peer_batch_size: 8,
            diagnostics: false,
            ordered_apply: false,
            peer_message_stream: true,
            pipelined_durability: true,
            max_speculative_proposals: 1,
            max_inflight_appends: 8,
            combine_peer_proposals: false,
            durable_completion_priority: true,
            openraft_async_flush: false,
        },
        name: "test",
        engine: PriorityEngine { observed },
        application: application::Application::Inline(model),
        dispatched: 0,
        outbound: BTreeMap::new(),
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
    }
}

#[test]
fn completion_priority_looks_past_a_bounded_ready_write_prefix() {
    let path =
        std::env::temp_dir().join(format!("commit-priority-lookahead-{}", std::process::id()));
    let (observed, order) = mpsc::channel();
    let mut state = priority_state(&path, observed, 2);
    let (tx, rx) = mpsc::sync_channel(8);
    tx.send(sequenced_write("one", 1)).unwrap();
    tx.send(sequenced_write("two", 2)).unwrap();
    tx.send(sequenced_write("three", 3)).unwrap();
    tx.send(peer(7)).unwrap();
    drop(tx);

    state.run(rx).unwrap();
    assert_eq!(order.recv_timeout(Duration::from_secs(1)).unwrap(), "peer");
    assert_eq!(
        order.recv_timeout(Duration::from_secs(1)).unwrap(),
        "proposal"
    );
    assert_eq!(
        order.recv_timeout(Duration::from_secs(1)).unwrap(),
        "proposal"
    );
    assert_eq!(state.prioritized_client_inputs_bypassed, 1);
    drop(state);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn completion_priority_does_not_look_past_more_than_one_batch_of_writes() {
    let path = std::env::temp_dir().join(format!("commit-priority-bound-{}", std::process::id()));
    let (observed, order) = mpsc::channel();
    let mut state = priority_state(&path, observed, 2);
    let (tx, rx) = mpsc::sync_channel(8);
    for sequence in 1..=5 {
        tx.send(sequenced_write(&format!("key-{sequence}"), sequence))
            .unwrap();
    }
    tx.send(peer(7)).unwrap();
    drop(tx);

    state.run(rx).unwrap();
    assert_eq!(
        order.recv_timeout(Duration::from_secs(1)).unwrap(),
        "proposal"
    );
    assert_eq!(order.recv_timeout(Duration::from_secs(1)).unwrap(), "peer");
    assert_eq!(
        order.recv_timeout(Duration::from_secs(1)).unwrap(),
        "proposal"
    );
    assert_eq!(
        order.recv_timeout(Duration::from_secs(1)).unwrap(),
        "proposal"
    );
    assert_eq!(state.prioritized_client_inputs_bypassed, 1);
    drop(state);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn completion_priority_does_not_cross_a_non_execute_client() {
    let path = std::env::temp_dir().join(format!(
        "commit-priority-client-boundary-{}",
        std::process::id()
    ));
    let (observed, order) = mpsc::channel();
    let mut state = priority_state(&path, observed, 2);
    let (tx, rx) = mpsc::sync_channel(8);
    tx.send(sequenced_write("one", 1)).unwrap();
    tx.send(sequenced_write("two", 2)).unwrap();
    let (reply, _response) = oneshot::channel();
    tx.send(Input::Client(
        Request {
            op: "status".into(),
            command: None,
        },
        reply,
        None,
    ))
    .unwrap();
    tx.send(peer(7)).unwrap();
    drop(tx);

    state.run(rx).unwrap();
    assert_eq!(
        order.recv_timeout(Duration::from_secs(1)).unwrap(),
        "proposal"
    );
    assert_eq!(order.recv_timeout(Duration::from_secs(1)).unwrap(), "peer");
    assert_eq!(state.prioritized_peer_batches, 0);
    assert_eq!(state.prioritized_client_inputs_bypassed, 0);
    drop(state);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn completion_priority_processes_safe_ack_before_ready_write() {
    let path = std::env::temp_dir().join(format!("commit-priority-{}", std::process::id()));
    let (observed, order) = mpsc::channel();
    let mut state = priority_state(&path, observed, 8);
    let (tx, rx) = mpsc::sync_channel(8);
    tx.send(write("key")).unwrap();
    tx.send(peer(7)).unwrap();
    drop(tx);

    state.run(rx).unwrap();
    assert_eq!(order.recv_timeout(Duration::from_secs(1)).unwrap(), "peer");
    assert_eq!(
        order.recv_timeout(Duration::from_secs(1)).unwrap(),
        "proposal"
    );
    assert_eq!(state.prioritized_peer_batches, 1);
    assert_eq!(state.prioritized_peer_events, 1);
    drop(state);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn completion_priority_preserves_unsafe_peer_after_ready_write() {
    let path =
        std::env::temp_dir().join(format!("commit-priority-boundary-{}", std::process::id()));
    let (observed, order) = mpsc::channel();
    let mut state = priority_state(&path, observed, 8);
    let (tx, rx) = mpsc::sync_channel(8);
    tx.send(write("key")).unwrap();
    tx.send(peer(0)).unwrap();
    drop(tx);

    state.run(rx).unwrap();
    assert_eq!(
        order.recv_timeout(Duration::from_secs(1)).unwrap(),
        "proposal"
    );
    assert_eq!(order.recv_timeout(Duration::from_secs(1)).unwrap(), "peer");
    assert_eq!(state.prioritized_peer_batches, 0);
    drop(state);
    std::fs::remove_file(path).unwrap();
}
