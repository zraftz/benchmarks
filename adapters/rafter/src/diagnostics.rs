//! Empty append counters by causal input, separate from the deterministic state.
//! Logged reads in this benchmark use proposals; ReadIndex/transfer remain zero.
use rafter::{
    Input, Message, Output, ReplicationProgress, ReplicationState, ReplicationWindowProgress, Role,
};
use std::{collections::BTreeMap, time::Instant};

pub(super) struct ReplicationWindows {
    enabled: bool,
    observed_at: Option<Instant>,
    observed_ns: u64,
    any_full_ns: u64,
    full_follower_ns: u64,
    observations: u64,
    current: Vec<ReplicationWindowProgress>,
}
impl ReplicationWindows {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            observed_at: None,
            observed_ns: 0,
            any_full_ns: 0,
            full_follower_ns: 0,
            observations: 0,
            current: Vec::new(),
        }
    }
    pub fn observe(&mut self, windows: Vec<ReplicationWindowProgress>) {
        if !self.enabled {
            return;
        }
        let now = Instant::now();
        self.account_until(now);
        self.current = windows;
        self.observations = self.observations.saturating_add(1);
    }
    pub fn enabled(&self) -> bool {
        self.enabled
    }
    fn account_until(&mut self, now: Instant) {
        let Some(prior) = self.observed_at.replace(now) else {
            return;
        };
        let elapsed =
            u64::try_from(now.saturating_duration_since(prior).as_nanos()).unwrap_or(u64::MAX);
        let full = self.current.iter().filter(|window| window.full).count() as u64;
        self.observed_ns = self.observed_ns.saturating_add(elapsed);
        if full != 0 {
            self.any_full_ns = self.any_full_ns.saturating_add(elapsed);
            self.full_follower_ns = self
                .full_follower_ns
                .saturating_add(elapsed.saturating_mul(full));
        }
    }
    pub fn snapshot(&self) -> serde_json::Value {
        if !self.enabled {
            return serde_json::Value::Null;
        }
        let mut observed_ns = self.observed_ns;
        let mut any_full_ns = self.any_full_ns;
        let mut full_follower_ns = self.full_follower_ns;
        if let Some(prior) = self.observed_at {
            let elapsed = u64::try_from(prior.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let full = self.current.iter().filter(|window| window.full).count() as u64;
            observed_ns = observed_ns.saturating_add(elapsed);
            if full != 0 {
                any_full_ns = any_full_ns.saturating_add(elapsed);
                full_follower_ns = full_follower_ns.saturating_add(elapsed.saturating_mul(full));
            }
        }
        let windows = self
            .current
            .iter()
            .map(|window| {
                serde_json::json!({
                    "follower_id":window.follower_id.0,
                    "in_flight_batches":window.in_flight_batches,
                    "in_flight_bytes":window.in_flight_bytes,
                    "max_in_flight_batches":window.max_in_flight_batches,
                    "max_in_flight_bytes":window.max_in_flight_bytes,
                    "full":window.full,
                })
            })
            .collect::<Vec<_>>();
        serde_json::json!({
            "definition":"owner-observed state between consensus turns; full_follower_ns sums time across followers",
            "observations":self.observations,
            "observed_ns":observed_ns,
            "any_full_ns":any_full_ns,
            "full_follower_ns":full_follower_ns,
            "currently_full_followers":self.current.iter().filter(|window| window.full).count(),
            "windows":windows,
        })
    }
}

pub(super) fn origin(input: &Input, role: Role) -> &'static str {
    match input {
        Input::ClientProposal { .. } | Input::TrackedClientProposal { .. } => "proposal",
        Input::ReadIndex { .. } => "read_confirmation",
        Input::TransferLeadership { .. } => "leadership_transfer",
        Input::Tick if role == Role::Leader => "heartbeat",
        Input::Message {
            message: Message::AppendEntriesResponse(_) | Message::InstallSnapshotResponse(_),
            ..
        } => "probe_recovery",
        _ => "election_or_membership",
    }
}
pub(super) struct EmptyAppends {
    enabled: bool,
    counts: BTreeMap<&'static str, u64>,
}
impl EmptyAppends {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            counts: [
                "heartbeat",
                "read_confirmation",
                "leadership_transfer",
                "proposal_window_full",
                "proposal_waiting_probe",
                "proposal_other",
                "probe_recovery",
                "election_or_membership",
            ]
            .into_iter()
            .map(|key| (key, 0))
            .collect(),
        }
    }
    pub fn enabled(&self) -> bool {
        self.enabled
    }
    pub fn record(
        &mut self,
        outputs: &[Output],
        origin: &'static str,
        progress: &[ReplicationProgress],
    ) {
        if !self.enabled {
            return;
        }
        for output in outputs {
            let Output::Send {
                to,
                message: Message::AppendEntries(append),
            } = output
            else {
                continue;
            };
            if !append.entries.is_empty() {
                continue;
            }
            let reason = if origin == "proposal" {
                match progress
                    .iter()
                    .find(|p| p.follower_id == *to)
                    .map(|p| p.state)
                {
                    Some(ReplicationState::Replicating) => "proposal_window_full",
                    Some(ReplicationState::Probing) => "proposal_waiting_probe",
                    _ => "proposal_other",
                }
            } else {
                origin
            };
            *self.counts.entry(reason).or_default() += 1;
        }
    }
    pub fn snapshot(&self) -> serde_json::Value {
        if self.enabled {
            serde_json::json!(self.counts)
        } else {
            serde_json::Value::Null
        }
    }
}

#[cfg(test)]
#[path = "diagnostics_test.rs"]
mod tests;
