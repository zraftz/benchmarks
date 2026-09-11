//! Synchronous or one-operation persistence ownership behind the same actor contract.
use crate::storage::Node;
use anyhow::{bail, Result};
use rafter::{Input, NodeId, Output, Role, Term};
#[cfg(feature = "pipelined-durability")]
mod pipeline;

pub enum Driver {
    Direct(Box<Node>),
    #[cfg(feature = "pipelined-durability")]
    Pipeline(Box<pipeline::Pipeline>),
}
impl Driver {
    pub fn new(
        node: Node,
        enabled: bool,
        diagnostics: bool,
        max_speculative_proposals: usize,
    ) -> Result<Self> {
        #[cfg(feature = "pipelined-durability")]
        if enabled {
            return Ok(Self::Pipeline(Box::new(pipeline::Pipeline::new(
                node,
                diagnostics,
                max_speculative_proposals,
            )?)));
        }
        let _ = (diagnostics, max_speculative_proposals);
        if enabled {
            bail!("pipelined durability is unavailable in this build")
        }
        Ok(Self::Direct(Box::new(node)))
    }
    pub fn ready(&self) -> &Node {
        match self {
            Self::Direct(node) => node,
            #[cfg(feature = "pipelined-durability")]
            Self::Pipeline(state) => state
                .node
                .ready_node()
                .expect("actor completed persistence before consensus access"),
        }
    }
    pub fn current_term(&self) -> Term {
        match self {
            Self::Direct(node) => node.current_term(),
            #[cfg(feature = "pipelined-durability")]
            Self::Pipeline(state) => state.term,
        }
    }
    pub fn role(&self) -> Role {
        match self {
            Self::Direct(node) => node.role(),
            #[cfg(feature = "pipelined-durability")]
            Self::Pipeline(state) => state.role,
        }
    }
    pub fn leader_hint(&self) -> Option<NodeId> {
        match self {
            Self::Direct(node) => node.leader_hint(),
            #[cfg(feature = "pipelined-durability")]
            Self::Pipeline(state) => state.leader,
        }
    }
    pub fn step_batch(&mut self, inputs: Vec<Input>) -> Result<Vec<Output>> {
        match self {
            Self::Direct(node) => Ok(node.step_batch(inputs)?),
            #[cfg(feature = "pipelined-durability")]
            Self::Pipeline(state) => state.step(inputs),
        }
    }
    pub fn pending(&self) -> bool {
        match self {
            Self::Direct(_) => false,
            #[cfg(feature = "pipelined-durability")]
            Self::Pipeline(state) => state.node.pending_operation().is_some(),
        }
    }
    pub fn complete(&mut self) -> Result<Option<Vec<Output>>> {
        match self {
            Self::Direct(_) => Ok(None),
            #[cfg(feature = "pipelined-durability")]
            Self::Pipeline(state) => state.complete(),
        }
    }
    pub fn stats(&self) -> serde_json::Value {
        match self {
            Self::Direct(_) => serde_json::json!({"enabled":false}),
            #[cfg(feature = "pipelined-durability")]
            Self::Pipeline(state) => {
                let p = state.node.progress();
                serde_json::json!({"enabled":true, "accepted_index":p.accepted.0,
                    "submitted_index":p.submitted.0,"durable_index":p.durable.0,
                    "durable_commit_index":p.committed.0,"submitted_operations":state.submitted,
                    "completed_operations":state.completed,"max_outstanding_operations":1,
                    "max_speculative_proposals":state.max_speculative_proposals,
                    "synchronous_proposal_batches":state.synchronous_proposal_batches,
                    "synchronous_proposals":state.synchronous_proposals,
                    "speculative_proposal_batches":state.speculative_proposal_batches,
                    "speculative_proposals":state.speculative_proposals,
                    "combined_peer_proposal_batches":state.combined_peer_proposal_batches,
                    "combined_peer_proposals":state.combined_peer_proposals,
                    "proposal_batch_sizes":state.proposal_batch_sizes})
            }
        }
    }
    #[cfg(feature = "peer-group-commit")]
    pub fn native_counters(&self) -> Vec<(&'static str, rafter_storage::telemetry::Metric)> {
        #[allow(unused_mut)]
        let mut all = rafter_storage::telemetry::snapshot();
        #[cfg(feature = "pipelined-durability")]
        if let Self::Pipeline(state) = self {
            for ((name, total), (worker_name, worker)) in all.iter_mut().zip(&state.counters) {
                assert_eq!(name, worker_name);
                total.calls = total.calls.saturating_add(worker.calls);
                total.total_ns = total.total_ns.saturating_add(worker.total_ns);
                total.max_ns = total.max_ns.max(worker.max_ns);
                total
                    .buckets
                    .resize(total.buckets.len().max(worker.buckets.len()), 0);
                for (sum, count) in total.buckets.iter_mut().zip(&worker.buckets) {
                    *sum = sum.saturating_add(*count);
                }
            }
        }
        all
    }
}
