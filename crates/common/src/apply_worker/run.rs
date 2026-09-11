//! Worker loop: drain only already-ready application work, sync, then complete.
use super::*;
use std::collections::VecDeque;

pub(super) struct State<S, F> {
    pub store: S,
    pub rx: mpsc::Receiver<Work>,
    pub done: mpsc::Sender<Result<Completion>>,
    pub queries: Arc<AtomicUsize>,
    pub durable: Arc<AtomicU64>,
    pub applying: Arc<AtomicU64>,
    pub diagnostics: Diagnostics,
    pub wake: F,
}
impl<S: ApplicationStore, F: Fn()> State<S, F> {
    pub fn run(mut self) {
        let mut deferred = VecDeque::new();
        while let Some(work) = deferred.pop_front().or_else(|| self.rx.recv().ok()) {
            match work {
                Work::Query(mut query) => {
                    if query.dump {
                        query.response.info = Some(self.store.dump());
                    } else if let Some(info) = &mut query.response.info {
                        info["application"] = self.store.stats();
                        info["application_in_progress_index"] =
                            self.applying.load(Ordering::Acquire).into();
                    }
                    let _ = query.reply.send(query.response);
                    self.queries.fetch_sub(1, Ordering::Release);
                }
                Work::Apply(items) => {
                    let mut entries = Vec::new();
                    let mut queued = Vec::new();
                    let mut input = VecDeque::from(items);
                    let mut payload = 0;
                    loop {
                        while let Some(item) = input.pop_front() {
                            let size = payload_bytes(&item.entry);
                            if !entries.is_empty()
                                && (entries.len() == BATCH_ENTRIES || payload + size > BATCH_BYTES)
                            {
                                input.push_front(item);
                                break;
                            }
                            payload += size;
                            self.diagnostics
                                .elapsed("application_queue_ns", item.queued);
                            entries.push(item.entry);
                            queued.push(item.queued);
                        }
                        if !input.is_empty() {
                            deferred.push_front(Work::Apply(input.into()));
                            break;
                        }
                        if entries.len() == BATCH_ENTRIES {
                            break;
                        }
                        match self.rx.try_recv() {
                            Ok(Work::Apply(items)) => input = items.into(),
                            Ok(other) => {
                                deferred.push_front(other);
                                break;
                            }
                            Err(_) => break,
                        }
                    }
                    let last = entries.last().unwrap().index;
                    self.applying.store(last, Ordering::Release);
                    self.diagnostics
                        .observe("application_batch_entries", entries.len() as u64);
                    let result = self.store.apply(&entries).and_then(|outcomes| {
                        if outcomes.len() != entries.len() {
                            bail!("application outcome count mismatch")
                        }
                        self.durable.store(last, Ordering::Release);
                        self.applying.store(0, Ordering::Release);
                        let bytes = entries.iter().map(retained_bytes).sum();
                        Ok(Completion {
                            entries,
                            outcomes,
                            queued,
                            bytes,
                        })
                    });
                    let failed = result.is_err();
                    if self.done.send(result).is_err() {
                        return;
                    }
                    (self.wake)();
                    if failed {
                        return;
                    }
                }
            }
        }
    }
}
