//! Only bounded application proposals can prepare speculative replication work.
use crate::storage;
use anyhow::{anyhow, bail, Result};
use rafter::{ClientProposalInput, Input, NodeId, Output, Role, Term};
use rafter_runtime::pipelined::{
    PersistenceWorkerOptions, PersistenceWorkerTelemetry, PreparedProposals,
};
use std::collections::BTreeMap;

pub struct Pipeline {
    pub node: storage::Pipeline,
    worker: storage::PipelineWorker,
    pub term: Term,
    pub role: Role,
    pub leader: Option<NodeId>,
    pub counters: PersistenceWorkerTelemetry,
    pub submitted: u64,
    pub completed: u64,
    pub synchronous_proposal_batches: u64,
    pub synchronous_proposals: u64,
    pub speculative_proposal_batches: u64,
    pub speculative_proposals: u64,
    pub combined_peer_proposal_batches: u64,
    pub combined_peer_first_batches: u64,
    pub combined_proposal_first_batches: u64,
    pub combined_peer_events: u64,
    pub combined_peer_proposals: u64,
    pub proposal_batch_sizes: BTreeMap<usize, u64>,
    pub max_speculative_proposals: usize,
    diagnostics: bool,
}
impl Pipeline {
    pub fn new(
        node: storage::Node,
        diagnostics: bool,
        max_speculative_proposals: usize,
    ) -> Result<Self> {
        if !(1..=64).contains(&max_speculative_proposals) {
            bail!("max speculative proposals must be 1..64")
        }
        Ok(Self {
            term: node.current_term(),
            role: node.role(),
            leader: node.leader_hint(),
            node: storage::Pipeline::new(node),
            worker: storage::PipelineWorker::start(
                PersistenceWorkerOptions::new().with_storage_telemetry(diagnostics),
            )?,
            counters: Vec::new(),
            submitted: 0,
            completed: 0,
            synchronous_proposal_batches: 0,
            synchronous_proposals: 0,
            speculative_proposal_batches: 0,
            speculative_proposals: 0,
            combined_peer_proposal_batches: 0,
            combined_peer_first_batches: 0,
            combined_proposal_first_batches: 0,
            combined_peer_events: 0,
            combined_peer_proposals: 0,
            proposal_batch_sizes: BTreeMap::new(),
            max_speculative_proposals,
            diagnostics,
        })
    }
    fn refresh(&mut self) {
        if let Some(node) = self.node.ready_node() {
            self.term = node.current_term();
            self.role = node.role();
            self.leader = node.leader_hint();
        }
    }
    pub fn step(&mut self, inputs: Vec<Input>) -> Result<Vec<Output>> {
        let input_count = inputs.len();
        let proposal_first = matches!(inputs.first(), Some(Input::ClientProposal { .. }));
        let proposal_count = inputs
            .iter()
            .filter(|input| matches!(input, Input::ClientProposal { .. }))
            .count();
        let proposals_only = proposal_count != 0 && proposal_count == inputs.len();
        if self.diagnostics && proposal_count != 0 {
            *self.proposal_batch_sizes.entry(proposal_count).or_default() += 1;
        }
        let outputs = if proposals_only && proposal_count <= self.max_speculative_proposals {
            let proposals = inputs
                .into_iter()
                .map(|input| {
                    let Input::ClientProposal { payload } = input else {
                        unreachable!()
                    };
                    ClientProposalInput {
                        proposal_id: None,
                        payload,
                    }
                })
                .collect();
            match self.node.prepare_proposals(proposals)? {
                PreparedProposals::Durable(outputs) => outputs,
                PreparedProposals::Pending { replication, work } => {
                    self.worker.try_submit(work).map_err(|error| {
                        anyhow!("persistence worker request queue unavailable: {error}")
                    })?;
                    self.submitted += 1;
                    self.speculative_proposal_batches =
                        self.speculative_proposal_batches.saturating_add(1);
                    self.speculative_proposals = self
                        .speculative_proposals
                        .saturating_add(proposal_count as u64);
                    replication
                }
                _ => bail!("unsupported persistence preparation result"),
            }
        } else {
            let outputs = self.node.step_batch(inputs)?;
            if proposal_count != 0 {
                self.synchronous_proposal_batches =
                    self.synchronous_proposal_batches.saturating_add(1);
                self.synchronous_proposals = self
                    .synchronous_proposals
                    .saturating_add(proposal_count as u64);
                if !proposals_only {
                    self.combined_peer_proposal_batches =
                        self.combined_peer_proposal_batches.saturating_add(1);
                    if proposal_first {
                        self.combined_proposal_first_batches =
                            self.combined_proposal_first_batches.saturating_add(1);
                    } else {
                        self.combined_peer_first_batches =
                            self.combined_peer_first_batches.saturating_add(1);
                    }
                    self.combined_peer_events = self
                        .combined_peer_events
                        .saturating_add((input_count - proposal_count) as u64);
                    self.combined_peer_proposals = self
                        .combined_peer_proposals
                        .saturating_add(proposal_count as u64);
                }
            }
            outputs
        };
        self.refresh();
        Ok(outputs)
    }
    pub fn complete(&mut self) -> Result<Option<Vec<Output>>> {
        if self.node.pending_operation().is_none() {
            return Ok(None);
        }
        let (completion, counters) = self.worker.complete()?.into_parts();
        // The runtime checks generation and operation before restoring consensus ownership.
        let outputs = self.node.complete(completion)?;
        self.counters = counters;
        self.completed += 1;
        self.refresh();
        Ok(Some(outputs))
    }
}

#[cfg(test)]
#[path = "pipeline_test.rs"]
mod tests;
