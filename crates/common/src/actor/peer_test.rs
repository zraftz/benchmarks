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
