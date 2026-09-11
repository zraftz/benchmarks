//! Empty append counters by causal input, separate from the deterministic state.
//! Logged reads in this benchmark use proposals; ReadIndex/transfer remain zero.
use rafter::{Input, Message, Output, ReplicationProgress, ReplicationState, Role};
use std::collections::BTreeMap;

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
