//! Eligible sends can run on peer writers while one local persistence job completes.
//! The owner processes no further consensus input until the engine accepts its receipt.
use super::*;

impl<E: Engine> State<E> {
    pub(super) fn complete_persistence(&mut self) -> Result<()> {
        if !self.engine.persistence_pending() {
            return Ok(());
        }
        let blocked = self.diagnostics.start();
        let effects = self
            .engine
            .complete_persistence()?
            .ok_or_else(|| anyhow::anyhow!("pending Raft persistence produced no completion"))?;
        self.diagnostics
            .elapsed("owner_persistence_blocked_ns", blocked);
        if self.engine.persistence_pending() {
            bail!("Raft persistence completion did not release consensus ownership")
        }
        self.effects(effects)
    }
}

#[cfg(test)]
#[path = "persistence_test.rs"]
mod tests;
