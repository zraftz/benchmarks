//! Dispatch cursors are separate from the worker's durable application cursor.
use super::*;
use crate::{
    apply_worker::{ApplyWorker, Query, BATCH_BYTES, BATCH_ENTRIES, QUEUE_ENTRIES},
    model::{ApplicationSnapshot, Outcome},
};

fn snapshot_due(applied: u64, current: u64, interval: u64, node_id: u64) -> bool {
    if applied == 0 || interval == 0 {
        return false;
    }
    if current != 0 {
        return applied.saturating_sub(current) >= interval;
    }
    // Spread the first maintenance boundary across the fixed three-voter
    // benchmark group by log position, without adding a time delay.
    let phase = interval.saturating_mul(node_id.saturating_sub(1).min(2)) / 3;
    applied >= interval.saturating_add(phase)
}

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

    pub fn snapshot_if_idle(&self) -> Result<Option<ApplicationSnapshot>> {
        match self {
            Self::Inline(model) => Ok(Some(model.snapshot())),
            Self::Worker(worker) => worker.snapshot(),
        }
    }

    pub fn snapshot_index_if_idle(&self) -> Option<u64> {
        match self {
            Self::Inline(model) => Some(model.index),
            Self::Worker(worker) if !worker.is_busy() => Some(worker.applied_index()),
            Self::Worker(_) => None,
        }
    }

    pub fn install_snapshot(&mut self, snapshot: ApplicationSnapshot) -> Result<()> {
        match self {
            Self::Inline(model) => model.install_snapshot(snapshot),
            Self::Worker(worker) => worker.install_snapshot(snapshot),
        }
    }
}
impl<E: Engine> State<E> {
    pub(super) fn maybe_compact_snapshot(&mut self) -> Result<()> {
        let interval = self.config.snapshot_interval_entries;
        if interval == 0 || self.engine.persistence_pending() {
            return Ok(());
        }
        let current = self.engine.snapshot_index();
        let Some(applied) = self.application.snapshot_index_if_idle() else {
            return Ok(());
        };
        if !snapshot_due(applied, current, interval, self.config.id) {
            return Ok(());
        }
        let Some(snapshot) = self.application.snapshot_if_idle()? else {
            return Ok(());
        };
        if snapshot.applied_index != applied {
            bail!("application snapshot boundary changed while preparing snapshot")
        }
        if snapshot.applied_index > self.engine.committed_through()
            || snapshot.applied_index > self.dispatched
        {
            bail!("application snapshot boundary is ahead of committed or dispatched state")
        }
        let payload = snapshot.encode()?;
        let payload_bytes = payload.len() as u64;
        let started = Instant::now();
        self.engine
            .compact_snapshot(snapshot.applied_index, payload)?;
        let elapsed = started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        self.snapshot_compactions = self.snapshot_compactions.saturating_add(1);
        self.snapshot_compaction_total_ns =
            self.snapshot_compaction_total_ns.saturating_add(elapsed);
        self.snapshot_compaction_max_ns = self.snapshot_compaction_max_ns.max(elapsed);
        let bucket = 63 - elapsed.max(1).leading_zeros() as usize;
        self.snapshot_compaction_buckets_log2[bucket] =
            self.snapshot_compaction_buckets_log2[bucket].saturating_add(1);
        self.snapshot_payload_bytes = payload_bytes;
        Ok(())
    }

    pub(super) fn drain_application(&mut self) -> Result<()> {
        loop {
            let completion = match &self.application {
                Application::Inline(_) => return Ok(()),
                Application::Worker(worker) if !worker.is_busy() => return Ok(()),
                Application::Worker(worker) => worker.complete()?,
            };
            if let Some(last) = completion.entries.last() {
                self.engine.applied(last.index);
            }
            self.resolve_applies(completion.entries, completion.outcomes, completion.queued);
        }
    }

    pub(super) fn install_application_snapshot(
        &mut self,
        snapshot: ApplicationSnapshot,
    ) -> Result<()> {
        self.drain_application()?;
        let applied = snapshot.applied_index;
        self.application.install_snapshot(snapshot)?;
        self.dispatched = applied;
        self.engine.applied(applied);
        self.application_snapshots_installed =
            self.application_snapshots_installed.saturating_add(1);

        let pending = std::mem::take(&mut self.pending);
        self.pending_count = 0;
        for (_, (command, replies)) in pending {
            self.diagnostics.abandon_operation(&command);
            for reply in replies {
                let _ = reply.send(Reply::status("unknown"));
            }
        }
        Ok(())
    }

    pub(super) fn dispatch_applies(&mut self) -> Result<()> {
        if self.engine.persistence_pending() {
            return Ok(());
        }
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
        mut queued: Option<Vec<Option<Instant>>>,
    ) {
        debug_assert!(
            queued
                .as_ref()
                .is_none_or(|queued| queued.len() == entries.len()),
            "application queue timestamp count mismatch"
        );
        for (position, (entry, outcome)) in entries.into_iter().zip(outcomes).enumerate() {
            let queued = queued
                .as_mut()
                .and_then(|queued| queued.get_mut(position))
                .and_then(Option::take);
            if let (Some(c), Some(result)) = (entry.command, outcome) {
                if let Some((submitted, replies)) = self.pending.remove(&c.identity()) {
                    self.pending_count -= replies.len();
                    let mut completed = false;
                    let matching = submitted == c;
                    let mut matching_result = Some(result);
                    let mut replies = replies.into_iter().peekable();
                    while let Some(reply) = replies.next() {
                        let response = if matching {
                            if replies.peek().is_none() {
                                matching_result
                                    .take()
                                    .expect("matching result is consumed by the final reply")
                            } else {
                                matching_result
                                    .as_ref()
                                    .expect("matching result remains before the final reply")
                                    .clone()
                            }
                        } else {
                            Outcome {
                                error: Some("identity_conflict".into()),
                                ..Default::default()
                            }
                        };
                        let client_completion = self.diagnostics.start();
                        let sent = reply.send(Reply::applied(response)).is_ok();
                        self.diagnostics
                            .elapsed("application_dispatch_to_client_completion_ns", queued);
                        if sent && !completed {
                            if let Some(client_completion) = client_completion {
                                self.diagnostics.client_completed_at(&c, client_completion);
                            }
                            completed = true;
                        }
                    }
                    if !completed {
                        self.diagnostics.abandon_operation(&c);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "application_test.rs"]
mod tests;
