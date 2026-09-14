//! Benchmark integration for Rafter's public bounded application worker.
//! Credits cover queued, in-progress, and completed-but-unconsumed entries.
//! The Raft owner never blocks submitting work or receiving a completion.
use crate::{
    diagnostics::Diagnostics,
    model::{ApplicationSnapshot, Applied, DurableModel, EncodedApplicationSnapshot, Outcome},
    Reply,
};
use anyhow::{bail, Result};
use rafter::LogIndex;
use rafter_runtime::application::{
    ApplicationEntry, ApplicationEvent, ApplicationFailureKind,
    ApplicationWorker as RafterApplicationWorker, ApplicationWorkerOptions, DurableApplication,
};
use std::{
    collections::{BTreeMap, VecDeque},
    error::Error,
    fmt,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, PoisonError, TryLockError,
    },
    time::Instant,
};
use tokio::sync::oneshot;

pub const BATCH_ENTRIES: usize = 64;
pub const BATCH_BYTES: usize = 256 * 1024;
pub const QUEUE_ENTRIES: usize = 4096;
pub const QUEUE_BYTES: usize = 16 * 1024 * 1024;
const QUERY_LIMIT: usize = 16;

pub trait ApplicationStore: Send + 'static {
    fn apply(&mut self, entries: &[Applied]) -> Result<Vec<Option<Outcome>>>;
    fn index(&self) -> u64;
    fn stats(&self) -> serde_json::Value;
    fn dump(&self) -> serde_json::Value;
    fn snapshot(&self) -> Result<ApplicationSnapshot> {
        bail!("application snapshots are unsupported by this store")
    }
    fn encode_snapshot(&self) -> Result<EncodedApplicationSnapshot> {
        let snapshot = self.snapshot()?;
        Ok(EncodedApplicationSnapshot {
            applied_index: snapshot.applied_index,
            payload: snapshot.encode()?,
        })
    }
    fn maintain_snapshot(&mut self, _snapshot: EncodedApplicationSnapshot) -> Result<()> {
        bail!("application snapshot checkpointing is unsupported by this store")
    }
    fn install_snapshot(&mut self, _snapshot: ApplicationSnapshot) -> Result<()> {
        bail!("application snapshots are unsupported by this store")
    }
}
impl ApplicationStore for DurableModel {
    fn apply(&mut self, entries: &[Applied]) -> Result<Vec<Option<Outcome>>> {
        self.apply(entries)
    }
    fn index(&self) -> u64 {
        self.index
    }
    fn stats(&self) -> serde_json::Value {
        self.stats()
    }
    fn dump(&self) -> serde_json::Value {
        serde_json::json!({"values":self.model.values,"applied_index":self.index})
    }
    fn snapshot(&self) -> Result<ApplicationSnapshot> {
        Ok(self.snapshot())
    }
    fn encode_snapshot(&self) -> Result<EncodedApplicationSnapshot> {
        DurableModel::encode_snapshot(self)
    }
    fn maintain_snapshot(&mut self, snapshot: EncodedApplicationSnapshot) -> Result<()> {
        self.maintain_checkpoint_snapshot(&snapshot)?;
        self.recycle_snapshot_payload(snapshot.payload);
        Ok(())
    }
    fn install_snapshot(&mut self, snapshot: ApplicationSnapshot) -> Result<()> {
        self.install_snapshot(snapshot)
    }
}

pub fn payload_bytes(entry: &Applied) -> usize {
    128 + entry.command.as_ref().map_or(0, |c| {
        c.client.len()
            + c.kind.len()
            + c.key.len()
            + c.value.len()
            + c.expected.as_ref().map_or(0, String::len)
    })
}
fn retained_bytes(entry: &Applied) -> usize {
    // Include the bounded result value even for a short get/CAS request.
    payload_bytes(entry)
        .saturating_mul(2)
        .saturating_add(64 * 1024)
}

impl ApplicationEntry for Applied {
    fn log_index(&self) -> LogIndex {
        LogIndex(self.index)
    }

    fn retained_bytes(&self) -> usize {
        retained_bytes(self)
    }

    fn batch_bytes(&self) -> usize {
        payload_bytes(self)
    }
}

#[derive(Debug)]
struct StoreError(anyhow::Error);

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:#}", self.0)
    }
}

impl Error for StoreError {}

type SharedStore = Arc<Mutex<Box<dyn ApplicationStore>>>;
type Queued = Arc<Mutex<BTreeMap<u64, Instant>>>;

struct WorkerStore {
    store: SharedStore,
    queued: Option<Queued>,
    diagnostics: Diagnostics,
}

impl DurableApplication<Applied> for WorkerStore {
    type Outcome = Option<Outcome>;
    type Error = StoreError;

    fn applied_through(&self) -> LogIndex {
        let store = self.store.lock().unwrap_or_else(PoisonError::into_inner);
        LogIndex(store.index())
    }

    fn apply(
        &mut self,
        entries: &[Applied],
    ) -> std::result::Result<Vec<Self::Outcome>, Self::Error> {
        if let Some(queued) = &self.queued {
            let queued = queued.lock().unwrap_or_else(PoisonError::into_inner);
            for entry in entries {
                self.diagnostics
                    .elapsed("application_queue_ns", queued.get(&entry.index).copied());
            }
        }
        self.diagnostics
            .observe("application_batch_entries", entries.len() as u64);
        self.diagnostics.application_started(entries);
        let expected_index = entries.last().map_or(0, |entry| entry.index);
        let outcomes = {
            let mut store = self.store.lock().unwrap_or_else(PoisonError::into_inner);
            let outcomes = store.apply(entries).map_err(StoreError)?;
            if outcomes.len() != entries.len() {
                return Err(StoreError(anyhow::anyhow!(
                    "application outcome count mismatch: expected {}, got {}",
                    entries.len(),
                    outcomes.len()
                )));
            }
            if store.index() != expected_index {
                return Err(StoreError(anyhow::anyhow!(
                    "application durable floor mismatch: expected {expected_index}, got {}",
                    store.index()
                )));
            }
            outcomes
        };
        self.diagnostics.application_durable(entries);
        Ok(outcomes)
    }
}

pub struct Completion {
    pub entries: Vec<Applied>,
    pub outcomes: Vec<Option<Outcome>>,
    pub queued: Option<Vec<Option<Instant>>>,
}

pub struct Query {
    pub reply: oneshot::Sender<Reply>,
    pub response: Reply,
    pub dump: bool,
}

struct PendingQuery {
    durable_fence: u64,
    query: Query,
}

pub struct ApplyWorker {
    worker: Option<RafterApplicationWorker<Applied, WorkerStore>>,
    store: SharedStore,
    queued: Option<Queued>,
    queries: Mutex<VecDeque<PendingQuery>>,
    query_count: AtomicUsize,
    accepted_through: AtomicU64,
    diagnostics: Diagnostics,
    wake: Arc<dyn Fn() + Send + Sync>,
}

impl ApplyWorker {
    pub fn start(
        store: impl ApplicationStore,
        diagnostics: Diagnostics,
        wake: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        let applied = store.index();
        let store: SharedStore = Arc::new(Mutex::new(Box::new(store)));
        let queued = diagnostics
            .enabled()
            .then(|| Arc::new(Mutex::new(BTreeMap::new())));
        let worker_store = WorkerStore {
            store: Arc::clone(&store),
            queued: queued.clone(),
            diagnostics: diagnostics.clone(),
        };
        let options = ApplicationWorkerOptions::new().with_limits(
            QUEUE_ENTRIES,
            QUEUE_BYTES,
            BATCH_ENTRIES,
            BATCH_BYTES,
        );
        let wake: Arc<dyn Fn() + Send + Sync> = Arc::new(wake);
        let worker_wake = Arc::clone(&wake);
        let worker = RafterApplicationWorker::start(worker_store, options, move || worker_wake())
            .expect("failed to start Rafter application worker");
        Self {
            worker: Some(worker),
            store,
            queued,
            queries: Mutex::new(VecDeque::new()),
            query_count: AtomicUsize::new(0),
            accepted_through: AtomicU64::new(applied),
            diagnostics,
            wake,
        }
    }

    fn worker(&self) -> &RafterApplicationWorker<Applied, WorkerStore> {
        self.worker
            .as_ref()
            .expect("application worker is present while the actor is live")
    }

    pub fn applied_index(&self) -> u64 {
        self.worker().durable_through().0
    }

    pub fn applying_index(&self) -> u64 {
        self.worker().applying_through().0
    }

    pub fn available(&self) -> (usize, usize) {
        self.worker().available()
    }

    pub fn has_capacity(&self) -> bool {
        let (entries, bytes) = self.available();
        entries > BATCH_ENTRIES && bytes > BATCH_BYTES
    }

    pub fn try_submit(&self, entries: Vec<Applied>) -> Result<bool> {
        if entries.is_empty() {
            return Ok(true);
        }
        if entries
            .iter()
            .any(|entry| payload_bytes(entry) > BATCH_BYTES)
        {
            bail!("application entry exceeds decoded byte cap")
        }
        for pair in entries.windows(2) {
            if pair[0].index.checked_add(1) != Some(pair[1].index) {
                bail!(
                    "application submission is not contiguous: {} then {}",
                    pair[0].index,
                    pair[1].index
                )
            }
        }
        let totals = entries
            .iter()
            .try_fold((0usize, 0usize), |(retained, batch), entry| {
                Some((
                    retained.checked_add(retained_bytes(entry))?,
                    batch.checked_add(payload_bytes(entry))?,
                ))
            });
        let (available_entries, available_bytes) = self.worker().available();
        let Some((retained, batch_bytes)) = totals else {
            return Ok(false);
        };
        if entries.len() > available_entries || retained > available_bytes {
            return Ok(false);
        }

        let final_index = entries.last().expect("nonempty submission").index;
        let mut queued = self
            .queued
            .as_ref()
            .map(|queued| queued.lock().unwrap_or_else(PoisonError::into_inner));
        if let Some(queued) = &mut queued {
            if entries
                .iter()
                .any(|entry| queued.contains_key(&entry.index))
            {
                bail!("application submission reused an in-flight log index")
            }
            for entry in &entries {
                queued.insert(
                    entry.index,
                    self.diagnostics
                        .start()
                        .expect("diagnostic queue exists only when diagnostics are enabled"),
                );
            }
        }
        self.diagnostics.application_dispatched(&entries);

        if entries.len() <= BATCH_ENTRIES && batch_bytes <= BATCH_BYTES {
            self.submit_chunk(entries)?;
        } else {
            let mut chunk = Vec::new();
            let mut chunk_bytes = 0usize;
            for entry in entries {
                let bytes = payload_bytes(&entry);
                if !chunk.is_empty()
                    && (chunk.len() == BATCH_ENTRIES
                        || chunk_bytes.saturating_add(bytes) > BATCH_BYTES)
                {
                    self.submit_chunk(std::mem::take(&mut chunk))?;
                    chunk_bytes = 0;
                }
                chunk_bytes += bytes;
                chunk.push(entry);
            }
            if !chunk.is_empty() {
                self.submit_chunk(chunk)?;
            }
        }
        self.accepted_through.store(final_index, Ordering::Release);
        drop(queued);
        Ok(true)
    }

    fn submit_chunk(&self, entries: Vec<Applied>) -> Result<()> {
        self.worker().try_submit(entries).map_err(|error| {
            let rejection = error.rejection();
            anyhow::anyhow!("Rafter application worker refused prepared work: {rejection:?}")
        })
    }

    pub fn poll(&self) -> Result<Option<Completion>> {
        let event = self.worker().try_complete().map_err(|error| {
            self.fail_queries(error.to_string());
            anyhow::anyhow!(error)
        })?;
        event.map(|event| self.handle_event(event)).transpose()
    }

    pub fn complete(&self) -> Result<Completion> {
        let event = self.worker().complete().map_err(|error| {
            self.fail_queries(error.to_string());
            anyhow::anyhow!(error)
        })?;
        self.handle_event(event)
    }

    fn handle_event(
        &self,
        event: ApplicationEvent<Applied, Option<Outcome>, StoreError>,
    ) -> Result<Completion> {
        match event {
            ApplicationEvent::Applied(completion) => {
                let (entries, outcomes) = completion.into_parts();
                let starts = if let Some(queued) = &self.queued {
                    let mut queued = queued.lock().unwrap_or_else(PoisonError::into_inner);
                    Some(
                        entries
                            .iter()
                            .map(|entry| queued.remove(&entry.index))
                            .collect(),
                    )
                } else {
                    None
                };
                self.complete_queries()?;
                Ok(Completion {
                    entries,
                    outcomes,
                    queued: starts,
                })
            }
            ApplicationEvent::Failed(failure) => {
                let detail = match failure.kind() {
                    ApplicationFailureKind::Store(error) => error.to_string(),
                    other => format!("{other:?}"),
                };
                self.fail_queries(&detail);
                bail!("application worker failed: {detail}")
            }
            _ => {
                self.fail_queries("unsupported application worker event");
                bail!("unsupported application worker event")
            }
        }
    }

    pub fn is_busy(&self) -> bool {
        self.worker().is_busy()
    }

    pub fn snapshot(&self) -> Result<Option<ApplicationSnapshot>> {
        if self.is_busy() {
            return Ok(None);
        }
        let store = self
            .store
            .lock()
            .map_err(|_| anyhow::anyhow!("application store lock poisoned"))?;
        store.snapshot().map(Some)
    }

    pub fn encode_snapshot(&self) -> Result<Option<EncodedApplicationSnapshot>> {
        if self.is_busy() {
            return Ok(None);
        }
        let store = self
            .store
            .lock()
            .map_err(|_| anyhow::anyhow!("application store lock poisoned"))?;
        store.encode_snapshot().map(Some)
    }

    pub fn maintain_snapshot(&self, snapshot: EncodedApplicationSnapshot) -> Result<()> {
        if self.is_busy() {
            bail!("cannot checkpoint an application snapshot while work is in flight")
        }
        let mut store = self
            .store
            .lock()
            .map_err(|_| anyhow::anyhow!("application store lock poisoned"))?;
        store.maintain_snapshot(snapshot)
    }

    pub fn install_snapshot(&mut self, snapshot: ApplicationSnapshot) -> Result<()> {
        if self.is_busy() {
            bail!("cannot install an application snapshot while work is in flight")
        }
        self.complete_queries()?;
        if self.query_count.load(Ordering::Acquire) != 0 {
            bail!("cannot install an application snapshot with pending queries")
        }
        let worker_store = self
            .worker
            .as_mut()
            .expect("application worker is present while the actor is live")
            .shutdown_into_store()
            .map_err(|error| anyhow::anyhow!(error))?;
        {
            let mut store = worker_store
                .store
                .lock()
                .map_err(|_| anyhow::anyhow!("application store lock poisoned"))?;
            store.install_snapshot(snapshot)?;
        }
        let applied = worker_store
            .store
            .lock()
            .map_err(|_| anyhow::anyhow!("application store lock poisoned"))?
            .index();
        let options = ApplicationWorkerOptions::new().with_limits(
            QUEUE_ENTRIES,
            QUEUE_BYTES,
            BATCH_ENTRIES,
            BATCH_BYTES,
        );
        let wake = Arc::clone(&self.wake);
        self.worker = Some(
            RafterApplicationWorker::start(worker_store, options, move || wake())
                .map_err(|error| anyhow::anyhow!(error))?,
        );
        self.accepted_through.store(applied, Ordering::Release);
        if let Some(queued) = &self.queued {
            queued
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clear();
        }
        Ok(())
    }

    pub fn query(&self, query: Query) -> Result<()> {
        if !self.worker().is_accepting() {
            let _ = query.reply.send(Reply::error("application worker stopped"));
            bail!("application worker stopped")
        }
        let durable_fence = self.accepted_through.load(Ordering::Acquire);
        {
            let mut queries = self.queries.lock().unwrap_or_else(PoisonError::into_inner);
            if queries.len() >= QUERY_LIMIT {
                let _ = query.reply.send(Reply::status("overloaded"));
                return Ok(());
            }
            queries.push_back(PendingQuery {
                durable_fence,
                query,
            });
            self.query_count.store(queries.len(), Ordering::Release);
        }
        self.complete_queries()
    }

    fn complete_queries(&self) -> Result<()> {
        if self.query_count.load(Ordering::Acquire) == 0 {
            return Ok(());
        }
        let durable = self.applied_index();
        {
            let queries = self.queries.lock().unwrap_or_else(PoisonError::into_inner);
            if queries
                .front()
                .is_none_or(|query| query.durable_fence > durable)
            {
                return Ok(());
            }
        }
        let store = match self.store.try_lock() {
            Ok(store) => store,
            Err(TryLockError::WouldBlock) => return Ok(()),
            Err(TryLockError::Poisoned(_)) => bail!("application store lock poisoned"),
        };
        let applying = self.applying_index();
        let mut ready = Vec::new();
        {
            let mut queries = self.queries.lock().unwrap_or_else(PoisonError::into_inner);
            while queries
                .front()
                .is_some_and(|query| query.durable_fence <= durable)
            {
                ready.push(queries.pop_front().expect("front was present").query);
            }
            self.query_count.store(queries.len(), Ordering::Release);
        }
        for mut query in ready {
            if query.dump {
                query.response.info = Some(store.dump());
            } else if let Some(info) = &mut query.response.info {
                info["application"] = store.stats();
                info["application_in_progress_index"] = applying.into();
            }
            let _ = query.reply.send(query.response);
        }
        Ok(())
    }

    fn fail_queries(&self, detail: impl fmt::Display) {
        let pending = {
            let mut queries = self.queries.lock().unwrap_or_else(PoisonError::into_inner);
            let pending = queries.drain(..).collect::<Vec<_>>();
            self.query_count.store(0, Ordering::Release);
            pending
        };
        for query in pending {
            let _ = query.query.reply.send(Reply::error(&detail));
        }
    }
}

impl Drop for ApplyWorker {
    fn drop(&mut self) {
        self.fail_queries("application worker stopped");
    }
}

#[cfg(test)]
#[path = "apply_worker_test.rs"]
mod tests;
