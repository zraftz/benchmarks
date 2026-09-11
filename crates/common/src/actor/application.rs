//! Dispatch cursors are separate from the worker's durable application cursor.
use super::*;
use crate::{
    apply_worker::{ApplyWorker, Query, BATCH_BYTES, BATCH_ENTRIES, QUEUE_ENTRIES},
    model::Outcome,
};

pub(super) enum Application {
    Inline(DurableModel),
    Worker(ApplyWorker),
}
impl Application {
    pub fn backpressured(&self, last_log: u64, dispatched: u64) -> bool {
        match self {
            Self::Inline(_) => false,
            Self::Worker(worker) => {
                !worker.has_capacity()
                    || last_log.saturating_sub(worker.applied_index()) >= QUEUE_ENTRIES as u64
                    || last_log.saturating_sub(dispatched) >= BATCH_ENTRIES as u64 * 4
            }
        }
    }
    pub fn query(&self, mut query: Query) -> Result<()> {
        match self {
            Self::Worker(worker) => worker.query(query),
            Self::Inline(model) => {
                if query.dump {
                    query.response.info = Some(
                        serde_json::json!({"values":model.model.values,"applied_index":model.index}),
                    );
                } else if let Some(info) = &mut query.response.info {
                    info["application"] = model.stats();
                }
                let _ = query.reply.send(query.response);
                Ok(())
            }
        }
    }
}
impl<E: Engine> State<E> {
    pub(super) fn dispatch_applies(&mut self) -> Result<()> {
        let Application::Worker(worker) = &self.application else {
            return Ok(());
        };
        if self.dispatched >= self.engine.committed_through() {
            return Ok(());
        }
        let (available, _) = worker.available();
        if available == 0 {
            return Ok(());
        }
        let entries = self.engine.committed_entries(
            self.dispatched,
            available.min(BATCH_ENTRIES),
            BATCH_BYTES,
        )?;
        let last = entries.last().map(|entry| entry.index);
        if last.is_none() {
            bail!("committed application source did not advance")
        }
        if worker.try_submit(entries)? {
            self.dispatched = last.unwrap();
        }
        Ok(())
    }
    pub(super) fn complete_applies(&mut self) -> Result<()> {
        loop {
            let Application::Worker(worker) = &self.application else {
                return Ok(());
            };
            let Some(completion) = worker.poll()? else {
                return Ok(());
            };
            if let Some(last) = completion.entries.last() {
                self.engine.applied(last.index);
            }
            self.resolve_applies(completion.entries, completion.outcomes, completion.queued);
        }
    }
    pub(super) fn resolve_applies(
        &mut self,
        entries: Vec<Applied>,
        outcomes: Vec<Option<Outcome>>,
        queued: Vec<Option<Instant>>,
    ) {
        for ((entry, outcome), queued) in entries.into_iter().zip(outcomes).zip(queued) {
            if let (Some(c), Some(result)) = (entry.command, outcome) {
                if let Some((submitted, replies)) = self.pending.remove(&c.identity()) {
                    self.pending_count -= replies.len();
                    for reply in replies {
                        let response = if submitted == c {
                            result.clone()
                        } else {
                            Outcome {
                                error: Some("identity_conflict".into()),
                                ..Default::default()
                            }
                        };
                        let _ = reply.send(Reply::applied(response));
                        self.diagnostics
                            .elapsed("application_dispatch_to_client_completion_ns", queued);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "application_test.rs"]
mod tests;
