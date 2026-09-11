use super::*;
use rafter::{AppendEntries, LogIndex, NodeId, ReplicationWindowProgress, Term};

fn empty() -> Output {
    Output::Send {
        to: NodeId(2),
        message: Message::AppendEntries(AppendEntries {
            term: Term(1),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(1),
            prev_log_term: Term(1),
            entries: Vec::new().into(),
            leader_commit: LogIndex(1),
            sequence: 4,
        }),
    }
}
#[test]
fn proposal_empty_appends_distinguish_full_windows_from_unanswered_probes() {
    let mut counts = EmptyAppends::new(true);
    let mut progress = ReplicationProgress {
        follower_id: NodeId(2),
        match_index: LogIndex(1),
        next_index: LogIndex(2),
        state: ReplicationState::Replicating,
    };
    counts.record(&[empty()], "proposal", &[progress]);
    progress.state = ReplicationState::Probing;
    counts.record(&[empty()], "proposal", &[progress]);
    counts.record(&[empty()], "heartbeat", &[progress]);
    let snapshot = counts.snapshot();
    assert_eq!(snapshot["proposal_window_full"], 1);
    assert_eq!(snapshot["proposal_waiting_probe"], 1);
    assert_eq!(snapshot["heartbeat"], 1);
    assert_eq!(snapshot["read_confirmation"], 0);
}
#[test]
fn timing_runs_do_not_collect_empty_append_counters() {
    let mut counts = EmptyAppends::new(false);
    counts.record(&[empty()], "heartbeat", &[]);
    assert_eq!(counts.snapshot(), serde_json::Value::Null);
}
#[test]
fn replication_window_snapshot_reports_exact_bounded_usage() {
    let mut windows = ReplicationWindows::new(true);
    windows.observe(vec![ReplicationWindowProgress {
        follower_id: NodeId(2),
        in_flight_batches: 8,
        in_flight_bytes: 4096,
        max_in_flight_batches: 8,
        max_in_flight_bytes: 4096,
        full: true,
    }]);
    let snapshot = windows.snapshot();
    assert_eq!(snapshot["observations"], 1);
    assert_eq!(snapshot["currently_full_followers"], 1);
    assert_eq!(snapshot["windows"][0]["in_flight_batches"], 8);
    assert_eq!(snapshot["windows"][0]["full"], true);
}
