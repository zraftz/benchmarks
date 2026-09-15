use super::*;
use crate::net::{read_frame, RETAINED_PEER_FRAME_BYTES};
use tokio::{io::AsyncReadExt, net::TcpListener};

fn peer(address: String) -> Outbound {
    Outbound::start(
        address,
        1,
        true,
        Diagnostics::new(false),
        Arc::new(AtomicU64::new(0)),
    )
}
#[tokio::test]
async fn queue_is_bounded_in_events_and_bytes_before_the_writer_runs() {
    let events = peer("127.0.0.1:1".into());
    for _ in 0..QUEUE_EVENTS {
        events.try_send(vec![0], None).unwrap();
    }
    assert!(events.try_send(vec![0], None).is_err());
    let bytes = peer("127.0.0.1:1".into());
    for _ in 0..3 {
        bytes.try_send(vec![0; 4 * 1024 * 1024], None).unwrap();
    }
    assert!(bytes.try_send(vec![0; 4 * 1024 * 1024], None).is_err());
}
#[tokio::test]
async fn queued_old_generation_is_not_sent_after_leadership_changes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let out = peer(listener.local_addr().unwrap().to_string());
    out.try_send(b"old".to_vec(), None).unwrap();
    out.set_generation(1);
    out.try_send(b"new".to_vec(), None).unwrap();
    let (mut stream, _) = timeout(Duration::from_secs(1), listener.accept())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&read_frame(&mut stream).await.unwrap()[8..], b"new");
    assert_eq!(out.stats()["stale_messages_dropped"], 1);
}
#[tokio::test]
async fn unavailable_voter_does_not_block_the_other_writer() {
    let unavailable = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let missing = unavailable.local_addr().unwrap().to_string();
    drop(unavailable);
    let bad = peer(missing);
    for _ in 0..32 {
        bad.try_send(vec![0], None).unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let good = peer(listener.local_addr().unwrap().to_string());
    good.try_send(b"quorum".to_vec(), None).unwrap();
    let (mut stream, _) = timeout(Duration::from_secs(1), listener.accept())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&read_frame(&mut stream).await.unwrap()[8..], b"quorum");
}
#[tokio::test]
async fn failed_partial_write_reconnects_with_a_complete_fresh_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut connection = MessageConnection {
        address: listener.local_addr().unwrap().to_string(),
        from: 1,
        stream: None,
        frame: Vec::new(),
    };
    let receiver = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let mut prefix = [0; 16];
        first.read_exact(&mut prefix).await.unwrap();
        drop(first);
        let (mut second, _) = listener.accept().await.unwrap();
        read_frame(&mut second).await.unwrap()
    });
    assert!(connection.send(&vec![0; FRAME_LIMIT - 8]).await.is_err());
    assert!(connection.stream.is_none());
    assert!(connection.frame.capacity() <= RETAINED_PEER_FRAME_BYTES);
    connection.send(b"fresh").await.unwrap();
    assert_eq!(&receiver.await.unwrap()[8..], b"fresh");
    assert!(connection.frame.capacity() <= RETAINED_PEER_FRAME_BYTES);
}

#[tokio::test]
async fn message_connection_reuses_one_bounded_wire_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut connection = MessageConnection {
        address: listener.local_addr().unwrap().to_string(),
        from: 7,
        stream: None,
        frame: Vec::new(),
    };
    let receiver = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let first = read_frame(&mut stream).await.unwrap();
        let second = read_frame(&mut stream).await.unwrap();
        (first, second)
    });

    connection.send(b"first").await.unwrap();
    let capacity = connection.frame.capacity();
    connection.send(b"next").await.unwrap();
    assert_eq!(connection.frame.capacity(), capacity);
    assert!(capacity <= RETAINED_PEER_FRAME_BYTES);

    let (first, second) = receiver.await.unwrap();
    assert_eq!(&first[..8], &7u64.to_be_bytes());
    assert_eq!(&first[8..], b"first");
    assert_eq!(&second[..8], &7u64.to_be_bytes());
    assert_eq!(&second[8..], b"next");
}
#[tokio::test]
async fn stalled_reader_times_out_and_releases_queued_credits() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let out = peer(listener.local_addr().unwrap().to_string());
    out.try_send(vec![0; FRAME_LIMIT - 8], None).unwrap();
    out.try_send(vec![0; 4 * 1024 * 1024], None).unwrap();
    let (_stalled, _) = listener.accept().await.unwrap();
    timeout(Duration::from_secs(4), async {
        while out.counters.failures.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        while out.credit.available_permits() != QUEUE_BYTES {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
