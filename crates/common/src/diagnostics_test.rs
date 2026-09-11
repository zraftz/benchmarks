use super::*;

fn command(sequence: u64) -> Command {
    Command {
        client: "timeline-client".into(),
        sequence,
        kind: "put".into(),
        key: "key".into(),
        value: sequence.to_string(),
        expected: None,
    }
}

fn complete(diagnostics: &Diagnostics, command: &Command, index: u64) {
    let entry = Applied {
        index,
        command: Some(command.clone()),
        metadata: None,
    };
    diagnostics.proposals_submitted(std::slice::from_ref(command));
    diagnostics.raft_commit_observed(index, "engine_apply_effect");
    diagnostics.application_dispatched(std::slice::from_ref(&entry));
    diagnostics.application_started(std::slice::from_ref(&entry));
    diagnostics.application_durable(std::slice::from_ref(&entry));
    diagnostics.client_completed(command);
}

#[test]
fn completed_timeline_has_ordered_named_boundaries() {
    let diagnostics = Diagnostics::new(true);
    let command = command(1);
    diagnostics.admit_operation(&command, diagnostics.start());
    complete(&diagnostics, &command, 7);

    let snapshot = diagnostics.status_snapshot();
    let timelines = &snapshot["operation_timelines"];
    assert_eq!(timelines["operations_seen"], 1);
    assert_eq!(timelines["operations_sampled"], 1);
    assert_eq!(timelines["completed_seen"], 1);
    let retained = timelines["retained"].as_array().unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0]["sample_ordinal"], 1);
    assert_eq!(retained[0]["log_index"], 7);
    assert_eq!(retained[0]["commit_boundary"], "engine_apply_effect");
    let points = retained[0]["points_ns"].as_object().unwrap();
    assert_eq!(points.len(), 8);
    let ordered = [
        "client_ingress",
        "owner_admitted",
        "proposal_submitted",
        "raft_commit_observed",
        "application_dispatched",
        "application_started",
        "application_durable",
        "client_completion",
    ];
    let values = ordered
        .iter()
        .map(|name| points[*name].as_u64().unwrap())
        .collect::<Vec<_>>();
    assert!(values.windows(2).all(|pair| pair[0] <= pair[1]));
    assert_eq!(
        snapshot["metrics"]["sampled_raft_commit_observed_to_client_completion_ns"]["samples"],
        1
    );
}

#[test]
fn completed_timeline_retention_is_bounded() {
    let diagnostics = Diagnostics::new(true);
    let operations = OPERATION_TIMELINE_SAMPLE_EVERY * (OPERATION_TIMELINE_RETAINED as u64 + 17);
    for sequence in 1..=operations {
        let command = command(sequence);
        diagnostics.admit_operation(&command, diagnostics.start());
        complete(&diagnostics, &command, sequence);
    }

    let snapshot = diagnostics.status_snapshot();
    let timelines = &snapshot["operation_timelines"];
    assert_eq!(timelines["operations_seen"], operations);
    assert_eq!(
        timelines["completed_seen"],
        OPERATION_TIMELINE_RETAINED as u64 + 17
    );
    assert_eq!(
        timelines["retained"].as_array().unwrap().len(),
        OPERATION_TIMELINE_RETAINED
    );
    assert_eq!(timelines["in_flight"], 0);
    assert_eq!(timelines["commit_marks"], 0);
}

#[test]
fn disabled_diagnostics_do_not_retain_operations() {
    let diagnostics = Diagnostics::new(false);
    let command = command(1);
    diagnostics.admit_operation(&command, diagnostics.start());
    complete(&diagnostics, &command, 1);

    let snapshot = diagnostics.status_snapshot();
    assert_eq!(snapshot["operation_timelines"]["operations_seen"], 0);
    assert_eq!(
        snapshot["operation_timelines"]["retained"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn active_timelines_and_commit_marks_are_bounded() {
    let diagnostics = Diagnostics::new(true);
    let operations = OPERATION_TIMELINE_SAMPLE_EVERY * OPERATION_TIMELINE_IN_FLIGHT as u64 + 1;
    for sequence in 1..=operations {
        let command = command(sequence);
        diagnostics.admit_operation(&command, diagnostics.start());
    }
    for index in 1..=OPERATION_COMMIT_MARKS as u64 + 1 {
        diagnostics.raft_commit_observed(index, "engine_apply_effect");
    }

    let snapshot = diagnostics.status_snapshot();
    let timelines = &snapshot["operation_timelines"];
    assert_eq!(timelines["operations_sampled"], 257);
    assert_eq!(timelines["in_flight"], OPERATION_TIMELINE_IN_FLIGHT);
    assert_eq!(timelines["skipped_in_flight"], 1);
    assert_eq!(timelines["commit_marks"], OPERATION_COMMIT_MARKS);
    assert_eq!(timelines["evicted_commit_marks"], 1);
}
