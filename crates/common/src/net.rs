//! Bounded, persistent length-prefixed TCP. Plaintext, trusted benchmark networks only.
//! Peer frame body = sender u64 (big-endian) + implementation-native RPC bytes.
pub mod outbound;

use crate::{Config, Reply, Request, FRAME_LIMIT};
use anyhow::{bail, Result};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Semaphore},
    time::timeout,
};

#[async_trait]
pub trait Handler: Clone + Send + Sync + 'static {
    fn message_oriented(&self) -> bool {
        false
    }
    async fn client(&self, request: Request) -> Reply;
    async fn peer(&self, from: u64, data: Vec<u8>) -> Result<Vec<u8>>;
}
pub async fn read_frame(s: &mut TcpStream) -> Result<Vec<u8>> {
    let size = s.read_u32().await? as usize;
    if size > FRAME_LIMIT {
        bail!("frame limit exceeded");
    }
    let mut data = vec![0; size];
    s.read_exact(&mut data).await?;
    Ok(data)
}
async fn read_peer_frame(s: &mut TcpStream) -> Result<(u64, Vec<u8>)> {
    let size = s.read_u32().await? as usize;
    if size > FRAME_LIMIT {
        bail!("frame limit exceeded");
    }
    if size < 8 {
        bail!("peer identity missing");
    }
    let mut sender = [0; 8];
    s.read_exact(&mut sender).await?;
    let mut data = vec![0; size - sender.len()];
    s.read_exact(&mut data).await?;
    Ok((u64::from_be_bytes(sender), data))
}
pub async fn write_frame(s: &mut TcpStream, data: &[u8]) -> Result<()> {
    if data.len() > FRAME_LIMIT {
        bail!("frame limit exceeded");
    }
    let mut frame = Vec::with_capacity(4 + data.len());
    frame.extend_from_slice(&(data.len() as u32).to_be_bytes());
    frame.extend_from_slice(data);
    s.write_all(&frame).await?;
    Ok(())
}
#[derive(Clone)]
pub struct Rpc {
    address: String,
    from: u64,
    connection: Arc<Mutex<Option<TcpStream>>>,
}
impl Rpc {
    pub fn new(address: String, from: u64) -> Self {
        Self {
            address,
            from,
            connection: Arc::new(Mutex::new(None)),
        }
    }
    pub async fn call(&self, bytes: &[u8]) -> Result<Vec<u8>> {
        let mut slot = self.connection.lock().await;
        let result = timeout(Duration::from_secs(2), async {
            if slot.is_none() {
                let stream = TcpStream::connect(&self.address).await?;
                stream.set_nodelay(true)?;
                *slot = Some(stream);
            }
            let mut packet = Vec::with_capacity(8 + bytes.len());
            packet.extend_from_slice(&self.from.to_be_bytes());
            packet.extend_from_slice(bytes);
            let stream = slot.as_mut().unwrap();
            write_frame(stream, &packet).await?;
            read_frame(stream).await
        })
        .await;
        match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(e)) => {
                *slot = None;
                Err(e)
            }
            Err(e) => {
                *slot = None;
                Err(e.into())
            }
        }
    }
}
pub async fn serve<H: Handler>(config: Config, handler: H) -> Result<()> {
    if config.peer_message_stream && !handler.message_oriented() {
        bail!("this engine requires request/reply peer transport")
    }
    let clients = TcpListener::bind(&config.client).await?;
    let peers = TcpListener::bind(&config.peer).await?;
    let cp = Arc::new(Semaphore::new(1024));
    let pp = Arc::new(Semaphore::new(32));
    loop {
        tokio::select! {
            result=clients.accept()=>{
                let (mut s,_)=result?; s.set_nodelay(true)?;
                let Ok(permit)=cp.clone().try_acquire_owned() else {continue};
                let h=handler.clone();
                tokio::spawn(async move {
                    let _permit=permit;
                    loop {
                        let data=match timeout(Duration::from_secs(30),read_frame(&mut s)).await {Ok(Ok(d))=>d,_=>break};
                        let reply=match serde_json::from_slice::<Request>(&data) {Ok(q)=>h.client(q).await,Err(e)=>Reply::error(e)};
                        let Ok(bytes)=serde_json::to_vec(&reply) else {break};
                        if !matches!(timeout(Duration::from_secs(10),write_frame(&mut s,&bytes)).await,Ok(Ok(()))) {break;}
                    }
                });
            },
            result=peers.accept()=>{
                let (s,_)=result?;s.set_nodelay(true)?;
                let Ok(permit)=pp.clone().try_acquire_owned() else {continue};
                let h=handler.clone();let voters=config.peers.clone();let own=config.id;
                let message_stream=config.peer_message_stream;
                tokio::spawn(async move {
                    let _permit=permit;
                    let _ = serve_peer(s, h, voters, own, message_stream).await;
                });
            },
            _=tokio::signal::ctrl_c()=>return Ok(()),
        }
    }
}

async fn serve_peer<H: Handler>(
    mut stream: TcpStream,
    handler: H,
    voters: std::collections::BTreeMap<u64, String>,
    own: u64,
    message_stream: bool,
) -> Result<()> {
    loop {
        let (from, data) = timeout(Duration::from_secs(30), read_peer_frame(&mut stream)).await??;
        if from == own || !voters.contains_key(&from) {
            bail!("unknown peer")
        }
        let response = handler.peer(from, data).await?;
        if message_stream {
            if !response.is_empty() {
                bail!("message handler returned an RPC response")
            }
        } else {
            timeout(Duration::from_secs(10), write_frame(&mut stream, &response)).await??;
        }
    }
}

#[cfg(test)]
#[path = "net_test.rs"]
mod tests;
