//! Explicit backend selection; every mode retains synchronous durable receipts.
use rafter_runtime::DurableRaftNode;
#[cfg(feature = "wal-hard-state")]
pub use rafter_storage::durable_batch::WalRaftNodeStores as NodeStores;
#[cfg(feature = "wal-hard-state")]
use rafter_storage::durable_batch::{WalRaftHardStateStore as HardState, WalRaftLogSegment as Log};
#[cfg(not(any(feature = "journal-hard-state", feature = "wal-hard-state")))]
use rafter_storage::FileRaftHardStateStore as HardState;
#[cfg(not(feature = "wal-hard-state"))]
use rafter_storage::FileRaftLogSegment as Log;
#[cfg(not(any(feature = "journal-hard-state", feature = "wal-hard-state")))]
pub use rafter_storage::FileRaftNodeStores as NodeStores;
use rafter_storage::FileRaftSnapshotStore;
#[cfg(all(feature = "journal-hard-state", not(feature = "wal-hard-state")))]
use rafter_storage::JournalRaftHardStateStore as HardState;
#[cfg(all(feature = "journal-hard-state", not(feature = "wal-hard-state")))]
pub use rafter_storage::JournalRaftNodeStores as NodeStores;
use std::path::Path;

pub const HARD_STATE_BACKEND: &str = if cfg!(feature = "wal-hard-state") {
    "wal"
} else if cfg!(feature = "journal-hard-state") {
    "journal"
} else {
    "replace"
};
pub type Node = DurableRaftNode<HardState, Log, FileRaftSnapshotStore>;

pub fn open(directory: &Path, wal_reclamation_bytes: u64) -> anyhow::Result<NodeStores> {
    #[cfg(feature = "wal-hard-state")]
    {
        use rafter_storage::durable_batch::WalRaftNodeStoresOptions;
        use std::{io, num::NonZeroU64};

        let threshold = NonZeroU64::new(wal_reclamation_bytes)
            .ok_or_else(|| io::Error::other("WAL reclamation threshold must be nonzero"))?;
        Ok(NodeStores::open_with_options(
            directory,
            WalRaftNodeStoresOptions::new().with_reclamation_threshold_bytes(threshold),
        )?)
    }
    #[cfg(not(feature = "wal-hard-state"))]
    {
        let _ = wal_reclamation_bytes;
        Ok(NodeStores::open(directory)?)
    }
}

#[cfg(feature = "pipelined-durability")]
pub type Pipeline =
    rafter_runtime::pipelined::ThreadedPipelinedRaftNode<HardState, Log, FileRaftSnapshotStore>;
