//! Independent bounded peer writers. Socket delivery is never Raft durability.
use super::{prepare_peer_frame, release_oversized_peer_frame, Rpc};
use crate::{diagnostics::Diagnostics, FRAME_LIMIT};
use anyhow::{bail, Result};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    sync::{mpsc, OwnedSemaphorePermit, Semaphore},
    time::timeout,
};

const QUEUE_EVENTS: usize = 256;
const QUEUE_BYTES: usize = 16 * 1024 * 1024;
const MAX_AGE: Duration = Duration::from_secs(2);

struct Packet {
    data: Vec<u8>,
    generation: u64,
    created: Instant,
    queued: Option<Instant>,
    _credit: OwnedSemaphorePermit,
}
#[derive(Default)]
struct Counters {
    frames: AtomicU64,
    bytes: AtomicU64,
    expired: AtomicU64,
    failures: AtomicU64,
}
/// Reusable message-engine transport seam: one writer per remote voter, no retries.
/// On failure, discard that connection's queued epoch; Raft schedules fresh work.
pub struct Outbound {
    tx: mpsc::Sender<Packet>,
    credit: Arc<Semaphore>,
    generation: Arc<AtomicU64>,
    counters: Arc<Counters>,
}
impl Outbound {
    pub fn start(
        address: String,
        from: u64,
        message_stream: bool,
        diagnostics: Diagnostics,
        dropped: Arc<AtomicU64>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(QUEUE_EVENTS);
        let generation = Arc::new(AtomicU64::new(0));
        let counters = Arc::new(Counters::default());
        tokio::spawn(run(
            rx,
            address,
            from,
            message_stream,
            diagnostics,
            dropped,
            generation.clone(),
            counters.clone(),
        ));
        Self {
            tx,
            credit: Arc::new(Semaphore::new(QUEUE_BYTES)),
            generation,
            counters,
        }
    }
    pub fn set_generation(&self, generation: u64) {
        self.generation.store(generation, Ordering::Release);
    }
    pub fn try_send(&self, data: Vec<u8>, queued: Option<Instant>) -> Result<()> {
        if data.len() + 8 > FRAME_LIMIT || data.capacity() + 12 > QUEUE_BYTES {
            bail!("peer frame exceeds transport budget")
        }
        let credit = self
            .credit
            .clone()
            .try_acquire_many_owned((data.capacity() + 12) as u32)?;
        let packet = Packet {
            data,
            generation: self.generation.load(Ordering::Acquire),
            created: Instant::now(),
            queued,
            _credit: credit,
        };
        self.tx
            .try_send(packet)
            .map_err(|_| anyhow::anyhow!("peer queue full or stopped"))
    }
    pub fn stats(&self) -> serde_json::Value {
        serde_json::json!({"socket_frames_sent":self.counters.frames.load(Ordering::Relaxed),
            "socket_bytes_sent":self.counters.bytes.load(Ordering::Relaxed),
            "stale_messages_dropped":self.counters.expired.load(Ordering::Relaxed),
            "connection_failures":self.counters.failures.load(Ordering::Relaxed),
            "retained_bytes":QUEUE_BYTES-self.credit.available_permits()})
    }
}
#[allow(clippy::too_many_arguments)]
async fn run(
    mut rx: mpsc::Receiver<Packet>,
    address: String,
    from: u64,
    message_stream: bool,
    diagnostics: Diagnostics,
    dropped: Arc<AtomicU64>,
    generation: Arc<AtomicU64>,
    counters: Arc<Counters>,
) {
    let rpc = Rpc::new(address.clone(), from);
    let mut connection = MessageConnection {
        address,
        from,
        stream: None,
        frame: Vec::new(),
    };
    let mut connection_generation = 0;
    while let Some(packet) = rx.recv().await {
        let current = generation.load(Ordering::Acquire);
        if packet.generation != current || packet.created.elapsed() > MAX_AGE {
            dropped.fetch_add(1, Ordering::Relaxed);
            counters.expired.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        if connection_generation != current {
            connection.stream = None;
            connection_generation = current;
        }
        diagnostics.elapsed("peer_outbound_queue_ns", packet.queued);
        let result = if message_stream {
            connection.send(&packet.data).await
        } else {
            rpc.call(&packet.data).await.map(|_| ())
        };
        if result.is_err() {
            counters.failures.fetch_add(1, Ordering::Relaxed);
            dropped.fetch_add(1, Ordering::Relaxed);
            // No replay after a possibly partial write. A reconnect starts a fresh frame.
            for _ in 0..QUEUE_EVENTS {
                if rx.try_recv().is_err() {
                    break;
                }
                dropped.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            counters.frames.fetch_add(1, Ordering::Relaxed);
            counters
                .bytes
                .fetch_add((packet.data.len() + 12) as u64, Ordering::Relaxed);
        }
    }
}
struct MessageConnection {
    address: String,
    from: u64,
    stream: Option<TcpStream>,
    frame: Vec<u8>,
}
impl MessageConnection {
    async fn send(&mut self, bytes: &[u8]) -> Result<()> {
        prepare_peer_frame(&mut self.frame, self.from, bytes)?;
        let result = timeout(MAX_AGE, async {
            if self.stream.is_none() {
                let stream = TcpStream::connect(&self.address).await?;
                stream.set_nodelay(true)?;
                self.stream = Some(stream);
            }
            self.stream.as_mut().unwrap().write_all(&self.frame).await
        })
        .await;
        release_oversized_peer_frame(&mut self.frame);
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                self.stream = None;
                Err(error.into())
            }
            Err(error) => {
                self.stream = None;
                Err(error.into())
            }
        }
    }
}

#[cfg(test)]
#[path = "outbound_test.rs"]
mod tests;
