use super::*;
use tokio::sync::mpsc;

#[derive(Clone)]
struct Inbox(mpsc::Sender<Vec<u8>>);
#[async_trait]
impl Handler for Inbox {
    fn message_oriented(&self) -> bool {
        true
    }
    async fn client(&self, _: Request) -> Reply {
        Reply::status("unused")
    }
    async fn peer(&self, _: u64, data: Vec<u8>) -> Result<Vec<u8>> {
        self.0.try_send(data)?;
        Ok(Vec::new())
    }
}
async fn pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let connect = TcpStream::connect(listener.local_addr().unwrap());
    let (client, accepted) = tokio::join!(connect, listener.accept());
    (client.unwrap(), accepted.unwrap().0)
}
fn packet(body: &[u8]) -> Vec<u8> {
    [1_u64.to_be_bytes().as_slice(), body].concat()
}
#[tokio::test]
async fn message_connection_delivers_two_frames_without_transport_replies() {
    let (mut client, server) = pair().await;
    let (tx, mut rx) = mpsc::channel(2);
    let task = tokio::spawn(serve_peer(
        server,
        Inbox(tx),
        [(1, "peer".into())].into(),
        2,
        true,
    ));
    write_frame(&mut client, &packet(b"append one"))
        .await
        .unwrap();
    write_frame(&mut client, &packet(b"append two"))
        .await
        .unwrap();
    assert_eq!(rx.recv().await.unwrap(), b"append one");
    assert_eq!(rx.recv().await.unwrap(), b"append two");
    assert!(timeout(Duration::from_millis(20), read_frame(&mut client))
        .await
        .is_err());
    task.abort();
}
#[tokio::test]
async fn partial_frame_is_discarded_and_reconnect_starts_at_a_new_frame() {
    let (mut client, server) = pair().await;
    let (tx, mut rx) = mpsc::channel(2);
    let voters = [(1, "peer".into())].into();
    let first = tokio::spawn(serve_peer(server, Inbox(tx.clone()), voters, 2, true));
    client.write_all(&[0, 0, 0, 20, 0, 0]).await.unwrap();
    drop(client);
    assert!(first.await.unwrap().is_err());
    assert!(rx.try_recv().is_err());
    let (mut client, server) = pair().await;
    let second = tokio::spawn(serve_peer(
        server,
        Inbox(tx),
        [(1, "peer".into())].into(),
        2,
        true,
    ));
    write_frame(&mut client, &packet(b"fresh")).await.unwrap();
    assert_eq!(rx.recv().await.unwrap(), b"fresh");
    second.abort();
}
