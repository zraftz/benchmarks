//! Optional shared stage histograms and bounded operation traces.
//! Use separate diagnostic and timing runs.
use crate::model::{Applied, Command};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Instant,
};

pub const OPERATION_TIMELINE_SAMPLE_EVERY: u64 = 64;
pub const OPERATION_TIMELINE_RETAINED: usize = 256;
pub const OPERATION_TIMELINE_IN_FLIGHT: usize = 256;
pub const OPERATION_COMMIT_MARKS: usize = 8192;

#[derive(Clone, Debug, Default, Serialize)]
pub struct Metric {
    samples: u64,
    total: u64,
    max: u64,
    buckets_log2: Vec<u64>,
}

#[derive(Clone, Debug)]
struct CommitMark {
    at: Instant,
    boundary: &'static str,
}

#[derive(Clone, Debug)]
struct InFlightTimeline {
    client: String,
    sequence: u64,
    sample_ordinal: u64,
    ingress: Instant,
    owner_admitted: Instant,
    proposal_submitted: Option<Instant>,
    log_index: Option<u64>,
    commit: Option<CommitMark>,
    application_dispatched: Option<Instant>,
    application_started: Option<Instant>,
    application_durable: Option<Instant>,
}
impl InFlightTimeline {
    fn matches(&self, command: &Command) -> bool {
        self.sequence == command.sequence && self.client == command.client
    }

    fn complete(self, client_completion: Instant) -> Option<OperationTimeline> {
        let proposal_submitted = self.proposal_submitted?;
        let log_index = self.log_index?;
        let commit = self.commit?;
        let application_dispatched = self.application_dispatched?;
        let application_started = self.application_started?;
        let application_durable = self.application_durable?;
        let ordered = [
            self.ingress,
            self.owner_admitted,
            proposal_submitted,
            commit.at,
            application_dispatched,
            application_started,
            application_durable,
            client_completion,
        ];
        if !ordered.windows(2).all(|pair| pair[0] <= pair[1]) {
            // A retry can share a command identity with an older, already
            // committed duplicate log entry. Do not publish a timeline whose
            // stages cannot be attributed to one operation generation.
            return None;
        }
        let mut points_ns = BTreeMap::new();
        points_ns.insert("client_ingress", 0);
        points_ns.insert(
            "owner_admitted",
            elapsed_ns(self.ingress, self.owner_admitted),
        );
        points_ns.insert(
            "proposal_submitted",
            elapsed_ns(self.ingress, proposal_submitted),
        );
        points_ns.insert("raft_commit_observed", elapsed_ns(self.ingress, commit.at));
        points_ns.insert(
            "application_dispatched",
            elapsed_ns(self.ingress, application_dispatched),
        );
        points_ns.insert(
            "application_started",
            elapsed_ns(self.ingress, application_started),
        );
        points_ns.insert(
            "application_durable",
            elapsed_ns(self.ingress, application_durable),
        );
        points_ns.insert(
            "client_completion",
            elapsed_ns(self.ingress, client_completion),
        );
        Some(OperationTimeline {
            sample_ordinal: self.sample_ordinal,
            log_index,
            commit_boundary: commit.boundary,
            points_ns,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
struct OperationTimeline {
    sample_ordinal: u64,
    log_index: u64,
    commit_boundary: &'static str,
    points_ns: BTreeMap<&'static str, u64>,
}

#[derive(Debug, Default)]
struct TimelineState {
    operations_seen: u64,
    operations_sampled: u64,
    completed_seen: u64,
    abandoned: u64,
    skipped_in_flight: u64,
    discarded_incomplete: u64,
    evicted_commit_marks: u64,
    in_flight: Vec<InFlightTimeline>,
    commit_marks: BTreeMap<u64, CommitMark>,
    retained: Vec<OperationTimeline>,
}
impl TimelineState {
    fn retain(&mut self, timeline: OperationTimeline) {
        self.completed_seen = self.completed_seen.saturating_add(1);
        if self.retained.len() < OPERATION_TIMELINE_RETAINED {
            self.retained.push(timeline);
            return;
        }
        let random = mix64(timeline.sample_ordinal ^ self.completed_seen.rotate_left(17));
        let slot = random % self.completed_seen;
        if slot < OPERATION_TIMELINE_RETAINED as u64 {
            self.retained[slot as usize] = timeline;
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Diagnostics {
    enabled: bool,
    metrics: Arc<Mutex<BTreeMap<String, Metric>>>,
    timelines: Arc<Mutex<TimelineState>>,
}
impl Diagnostics {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            ..Self::default()
        }
    }
    pub fn start(&self) -> Option<Instant> {
        self.enabled.then(Instant::now)
    }
    pub fn enabled(&self) -> bool {
        self.enabled
    }
    pub fn elapsed(&self, name: &str, start: Option<Instant>) {
        if let Some(start) = start {
            self.observe(
                name,
                u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX),
            );
        }
    }
    pub fn observe(&self, name: &str, value: u64) {
        if !self.enabled {
            return;
        }
        let mut all = self.metrics.lock().unwrap();
        let metric = all.entry(name.to_owned()).or_default();
        metric.samples += 1;
        metric.total = metric.total.saturating_add(value);
        metric.max = metric.max.max(value);
        metric.buckets_log2.resize(64, 0);
        metric.buckets_log2[63 - value.max(1).leading_zeros() as usize] += 1;
    }
    /// Declares a metric before its optional path first executes.
    pub fn declare(&self, name: &str) {
        if self.enabled {
            self.metrics
                .lock()
                .unwrap()
                .entry(name.to_owned())
                .or_default();
        }
    }
    pub fn admit_operation(&self, command: &Command, ingress: Option<Instant>) {
        if !self.enabled {
            return;
        }
        let Some(ingress) = ingress else {
            return;
        };
        let owner_admitted = Instant::now();
        let mut state = self.timelines.lock().unwrap();
        state.operations_seen = state.operations_seen.saturating_add(1);
        let ordinal = state.operations_seen;
        if (ordinal - 1) % OPERATION_TIMELINE_SAMPLE_EVERY != 0 {
            return;
        }
        state.operations_sampled = state.operations_sampled.saturating_add(1);
        if state.in_flight.len() >= OPERATION_TIMELINE_IN_FLIGHT {
            state.skipped_in_flight = state.skipped_in_flight.saturating_add(1);
            return;
        }
        state.in_flight.push(InFlightTimeline {
            client: command.client.clone(),
            sequence: command.sequence,
            sample_ordinal: ordinal,
            ingress,
            owner_admitted,
            proposal_submitted: None,
            log_index: None,
            commit: None,
            application_dispatched: None,
            application_started: None,
            application_durable: None,
        });
    }
    pub fn proposals_submitted(&self, commands: &[Command]) {
        if !self.enabled {
            return;
        }
        let now = Instant::now();
        let mut state = self.timelines.lock().unwrap();
        for command in commands {
            if let Some(timeline) = state
                .in_flight
                .iter_mut()
                .find(|timeline| timeline.matches(command))
            {
                timeline.proposal_submitted.get_or_insert(now);
            }
        }
    }
    pub fn raft_commit_observed(&self, index: u64, boundary: &'static str) {
        if !self.enabled {
            return;
        }
        let at = Instant::now();
        let mut state = self.timelines.lock().unwrap();
        state
            .commit_marks
            .entry(index)
            .or_insert(CommitMark { at, boundary });
        while state.commit_marks.len() > OPERATION_COMMIT_MARKS {
            state.commit_marks.pop_first();
            state.evicted_commit_marks = state.evicted_commit_marks.saturating_add(1);
        }
    }
    pub fn application_dispatched(&self, entries: &[Applied]) {
        if !self.enabled {
            return;
        }
        let now = Instant::now();
        let mut state = self.timelines.lock().unwrap();
        for entry in entries {
            let commit = state.commit_marks.remove(&entry.index);
            let Some(command) = &entry.command else {
                continue;
            };
            if let Some(timeline) = state
                .in_flight
                .iter_mut()
                .find(|timeline| timeline.matches(command))
            {
                if timeline.log_index.is_none() {
                    timeline.log_index = Some(entry.index);
                    timeline.commit = commit;
                    timeline.application_dispatched = Some(now);
                }
            }
        }
    }
    pub fn application_started(&self, entries: &[Applied]) {
        self.mark_application(entries, ApplicationStage::Started);
    }
    pub fn application_durable(&self, entries: &[Applied]) {
        self.mark_application(entries, ApplicationStage::Durable);
    }
    fn mark_application(&self, entries: &[Applied], stage: ApplicationStage) {
        if !self.enabled {
            return;
        }
        let now = Instant::now();
        let mut state = self.timelines.lock().unwrap();
        for entry in entries {
            if let Some(timeline) = state
                .in_flight
                .iter_mut()
                .find(|timeline| timeline.log_index == Some(entry.index))
            {
                match stage {
                    ApplicationStage::Started => {
                        timeline.application_started.get_or_insert(now);
                    }
                    ApplicationStage::Durable => {
                        timeline.application_durable.get_or_insert(now);
                    }
                }
            }
        }
    }
    pub fn client_completed(&self, command: &Command) {
        if !self.enabled {
            return;
        }
        self.client_completed_at(command, Instant::now());
    }
    pub fn client_completed_at(&self, command: &Command, client_completion: Instant) {
        if !self.enabled {
            return;
        }
        let completed = {
            let mut state = self.timelines.lock().unwrap();
            let Some(position) = state
                .in_flight
                .iter()
                .position(|timeline| timeline.matches(command))
            else {
                return;
            };
            let timeline = state.in_flight.remove(position);
            let Some(completed) = timeline.complete(client_completion) else {
                state.discarded_incomplete = state.discarded_incomplete.saturating_add(1);
                return;
            };
            state.retain(completed.clone());
            completed
        };
        let point = |name| completed.points_ns.get(name).copied().unwrap_or_default();
        self.observe(
            "sampled_client_ingress_to_raft_commit_observed_ns",
            point("raft_commit_observed"),
        );
        self.observe(
            "sampled_raft_commit_observed_to_application_dispatched_ns",
            point("application_dispatched").saturating_sub(point("raft_commit_observed")),
        );
        self.observe(
            "sampled_application_dispatched_to_durable_ns",
            point("application_durable").saturating_sub(point("application_dispatched")),
        );
        self.observe(
            "sampled_application_durable_to_client_completion_ns",
            point("client_completion").saturating_sub(point("application_durable")),
        );
        self.observe(
            "sampled_raft_commit_observed_to_client_completion_ns",
            point("client_completion").saturating_sub(point("raft_commit_observed")),
        );
        self.observe(
            "sampled_client_ingress_to_client_completion_ns",
            point("client_completion"),
        );
    }
    pub fn abandon_operation(&self, command: &Command) {
        if !self.enabled {
            return;
        }
        let mut state = self.timelines.lock().unwrap();
        let before = state.in_flight.len();
        state
            .in_flight
            .retain(|timeline| !timeline.matches(command));
        if state.in_flight.len() != before {
            state.abandoned = state.abandoned.saturating_add(1);
        }
    }
    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"enabled":self.enabled,"metrics":*self.metrics.lock().unwrap()})
    }
    pub fn status_snapshot(&self) -> serde_json::Value {
        let timelines = self.timelines.lock().unwrap();
        let mut retained = timelines.retained.clone();
        retained.sort_by_key(|timeline| timeline.sample_ordinal);
        let operation_timelines = serde_json::json!({
            "schema":1,
            "sample_every":OPERATION_TIMELINE_SAMPLE_EVERY,
            "retained_limit":OPERATION_TIMELINE_RETAINED,
            "in_flight_limit":OPERATION_TIMELINE_IN_FLIGHT,
            "commit_mark_limit":OPERATION_COMMIT_MARKS,
            "retention":"deterministic reservoir over completed sampled operations",
            "operations_seen":timelines.operations_seen,
            "operations_sampled":timelines.operations_sampled,
            "completed_seen":timelines.completed_seen,
            "abandoned":timelines.abandoned,
            "skipped_in_flight":timelines.skipped_in_flight,
            "discarded_incomplete":timelines.discarded_incomplete,
            "evicted_commit_marks":timelines.evicted_commit_marks,
            "in_flight":timelines.in_flight.len(),
            "commit_marks":timelines.commit_marks.len(),
            "retained":retained,
        });
        drop(timelines);
        serde_json::json!({"enabled":self.enabled,"metrics":*self.metrics.lock().unwrap(),"operation_timelines":operation_timelines})
    }
}

#[derive(Clone, Copy)]
enum ApplicationStage {
    Started,
    Durable,
}

fn elapsed_ns(start: Instant, end: Instant) -> u64 {
    u64::try_from(end.saturating_duration_since(start).as_nanos()).unwrap_or(u64::MAX)
}

fn mix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
#[path = "diagnostics_test.rs"]
mod tests;
