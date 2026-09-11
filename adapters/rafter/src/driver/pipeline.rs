//! Only bounded application proposals can prepare speculative replication work.
use super::worker::{Counters, Worker};
use crate::storage;
use anyhow::{bail, Result};
use rafter::{ClientProposalInput, Input, NodeId, Output, Role, Term};
use rafter_runtime::pipelined::PreparedProposals;

/// Speculate only when the ready queue cannot already amortize persistence.
///
/// A larger ready batch takes the ordinary synchronous group-commit path. This
/// introduces no timer and keeps the runtime's single-owner durability fence.
pub(super) const MAX_SPECULATIVE_PROPOSALS: usize = 1;

pub struct Pipeline {
    pub node: storage::Pipeline,
    worker: Worker,
    pub term: Term,
    pub role: Role,
    pub leader: Option<NodeId>,
    pub counters: Counters,
    pub submitted: u64,
    pub completed: u64,
    pub synchronous_proposal_batches: u64,
    pub synchronous_proposals: u64,
}
impl Pipeline {
    pub fn new(node: storage::Node, diagnostics: bool) -> Result<Self> {
        Ok(Self {
            term: node.current_term(),
            role: node.role(),
            leader: node.leader_hint(),
            node: storage::Pipeline::new(node),
            worker: Worker::start(diagnostics)?,
            counters: Vec::new(),
            submitted: 0,
            completed: 0,
            synchronous_proposal_batches: 0,
            synchronous_proposals: 0,
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
        let proposal_count = (!inputs.is_empty()
            && inputs
                .iter()
                .all(|input| matches!(input, Input::ClientProposal { .. })))
        .then_some(inputs.len());
        let outputs = if proposal_count.is_some_and(|count| count <= MAX_SPECULATIVE_PROPOSALS) {
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
                    self.worker.submit(work)?;
                    self.submitted += 1;
                    replication
                }
                _ => bail!("unsupported persistence preparation result"),
            }
        } else {
            let outputs = self.node.step_batch(inputs)?;
            if let Some(count) = proposal_count {
                self.synchronous_proposal_batches =
                    self.synchronous_proposal_batches.saturating_add(1);
                self.synchronous_proposals =
                    self.synchronous_proposals.saturating_add(count as u64);
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
        let (completion, counters) = self.worker.complete()?;
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
