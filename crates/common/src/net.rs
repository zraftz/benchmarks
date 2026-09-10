//! Bounded, persistent length-prefixed TCP. Plaintext, trusted benchmark networks only.
//! Peer frame body = sender u64 (big-endian) + implementation-native RPC bytes.
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
pub async fn write_frame(s: &mut TcpStream, data: &[u8]) -> Result<()> {
    if data.len() > FRAME_LIMIT {
        bail!("frame limit exceeded");
    }
    s.write_u32(data.len() as u32).await?;
    s.write_all(data).await?;
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
                let (mut s,_)=result?;s.set_nodelay(true)?;
                let Ok(permit)=pp.clone().try_acquire_owned() else {continue};
                let h=handler.clone();let voters=config.peers.clone();let own=config.id;
                tokio::spawn(async move {
                    let _permit=permit;
                    loop {
                        let data=match timeout(Duration::from_secs(30),read_frame(&mut s)).await {Ok(Ok(d))=>d,_=>break};
                        if data.len()<8 {break;}
                        let from=u64::from_be_bytes(data[..8].try_into().unwrap());
                        if from==own || !voters.contains_key(&from) {break;}
                        let response=match h.peer(from,data[8..].to_vec()).await {Ok(r)=>r,Err(_)=>break};
                        if !matches!(timeout(Duration::from_secs(10),write_frame(&mut s,&response)).await,Ok(Ok(()))) {break;}
                    }
                });
            },
            _=tokio::signal::ctrl_c()=>return Ok(()),
        }
    }
}
