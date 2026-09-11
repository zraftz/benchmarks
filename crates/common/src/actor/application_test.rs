//! The Raft owner keeps processing peers while application persistence is held open.
use super::*;
use crate::apply_worker::ApplicationStore;
use std::sync::atomic::AtomicUsize;
static NEXT: AtomicUsize = AtomicUsize::new(0);

struct PausedStore {
    model: DurableModel,
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}
impl ApplicationStore for PausedStore {
    fn apply(&mut self, entries: &[Applied]) -> Result<Vec<Option<Outcome>>> {
        self.entered.send(()).unwrap();
        self.release.recv().unwrap();
        self.model.apply(entries)
    }
    fn index(&self) -> u64 {
        self.model.index
    }
    fn stats(&self) -> serde_json::Value {
        self.model.stats()
    }
    fn dump(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}
struct EngineProbe {
    peer_seen: mpsc::Sender<()>,
    entry: Applied,
}
impl Engine for EngineProbe {
    type Peer = ();
    type Gate = ();
    fn decode_peer(&self, _: u64, _: &[u8]) -> Result<()> {
        Ok(())
    }
    fn peer_gate(&self, _: usize, _: usize) {}
    fn admit_peer(_: &mut (), _: &()) -> bool {
        false
    }
    fn peer_batch(&mut self, _: Vec<()>) -> Result<Vec<Effect>> {
        self.peer_seen.send(()).unwrap();
        Ok(vec![])
    }
    fn leader(&self) -> Option<u64> {
        Some(1)
    }
    fn is_leader(&self) -> bool {
        true
    }
    fn tick(&mut self) -> Result<Vec<Effect>> {
        Ok(vec![])
    }
    fn propose(&mut self, _: Vec<Command>) -> Result<Vec<Effect>> {
        unreachable!()
    }
    fn stats(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
    fn committed_through(&self) -> u64 {
        1
    }
    fn last_index(&self) -> u64 {
        1
    }
    fn committed_entries(&self, after: u64, _: usize, _: usize) -> Result<Vec<Applied>> {
        Ok(if after == 0 {
            vec![self.entry.clone()]
        } else {
            vec![]
        })
    }
}
#[test]
fn peers_progress_during_ten_ms_apply_delay_but_client_waits_for_durable_completion() {
    let path = std::env::temp_dir().join(format!(
        "apply-owner-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let (entered, waiting) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    let (tx, rx) = mpsc::sync_channel(8);
    let tx = Arc::new(tx);
    let wake = Arc::downgrade(&tx);
    let store = PausedStore {
        model: DurableModel::open(&path).unwrap(),
        entered,
        release: resume,
    };
    let worker = ApplyWorker::start(store, Diagnostics::default(), move || {
        if let Some(tx) = wake.upgrade() {
            let _ = tx.try_send(Input::Wake);
        }
    });
    let c = Command {
        client: "c".into(),
        sequence: 1,
        kind: "put".into(),
        key: "k".into(),
        value: "durable".into(),
        expected: None,
    };
    let entry = Applied {
        index: 1,
        command: Some(c.clone()),
        metadata: None,
    };
    worker.try_submit(vec![entry.clone()]).unwrap();
    waiting.recv_timeout(Duration::from_secs(1)).unwrap();
    let (reply, mut response) = oneshot::channel();
    let mut pending = BTreeMap::new();
    pending.insert(c.identity(), (c, vec![reply]));
    let (peer_seen, observed) = mpsc::channel();
    let config = Config {
        id: 1,
        cluster: "test".into(),
        client: String::new(),
        peer: String::new(),
        peers: BTreeMap::new(),
        data_dir: path.clone(),
        tick_ms: 20,
        capacity: 4096,
        batch_size: 64,
        peer_batch_size: 1,
        diagnostics: false,
        ordered_apply: true,
        peer_message_stream: false,
        pipelined_durability: false,
        max_speculative_proposals: 1,
        combine_peer_proposals: false,
        openraft_async_flush: false,
    };
    let mut state = State {
        diagnostics: Diagnostics::default(),
        config,
        name: "test",
        engine: EngineProbe { peer_seen, entry },
        application: Application::Worker(worker),
        dispatched: 1,
        outbound: BTreeMap::new(),
        transport_epoch: (0, None),
        transport_generation: 0,
        dropped: Arc::new(AtomicU64::new(0)),
        pending,
        pending_count: 1,
        peer_batches: 0,
        peer_events: 0,
        peer_batch_sizes: BTreeMap::new(),
    };
    let owner = std::thread::spawn(move || state.run(rx));
    tx.send(Input::Peer(2, vec![], None)).unwrap();
    observed.recv_timeout(Duration::from_secs(1)).unwrap();
    std::thread::sleep(Duration::from_millis(10));
    assert!(matches!(
        response.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    release.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let result = loop {
        if let Ok(result) = response.try_recv() {
            break result;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(result.status, "ok");
    assert_eq!(result.result.unwrap().value.as_deref(), Some("durable"));
    drop(tx);
    owner.join().unwrap().unwrap();
    let reopened = DurableModel::open(&path).unwrap();
    assert_eq!(reopened.model.values["k"], "durable");
    drop(reopened);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn client_saturation_leaves_reserved_peer_inbox_capacity() {
    let (tx, rx) = mpsc::sync_channel(4);
    let slots = Arc::new(tokio::sync::Semaphore::new(3));
    let actor = Actor {
        tx: Arc::new(tx),
        diagnostics: Diagnostics::default(),
        client_slots: slots.clone(),
    };
    let mut clients = Vec::new();
    for _ in 0..3 {
        let actor = actor.clone();
        clients.push(tokio::spawn(async move {
            actor
                .client(Request {
                    op: "status".into(),
                    command: None,
                })
                .await
        }));
    }
    let deadline = Instant::now() + Duration::from_secs(1);
    while slots.available_permits() != 0 {
        assert!(Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    assert_eq!(
        actor
            .client(Request {
                op: "status".into(),
                command: None
            })
            .await
            .status,
        "overloaded"
    );
    actor.peer(2, vec![1]).await.unwrap();
    let mut peers = 0;
    for input in rx.try_iter() {
        match input {
            Input::Client(_, reply, _) => {
                let _ = reply.send(Reply::status("ok"));
            }
            Input::Peer(..) => peers += 1,
            Input::Wake => unreachable!(),
        }
    }
    assert_eq!(peers, 1);
    for client in clients {
        assert_eq!(client.await.unwrap().status, "ok");
    }
}
