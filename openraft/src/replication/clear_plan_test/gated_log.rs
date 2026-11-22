use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::sync::Arc;

use super::C;
use crate::OptionalSend;
use crate::RaftLogReader;
use crate::async_runtime::WatchReceiver;
use crate::async_runtime::WatchSender;
use crate::engine::testing::log_id;
use crate::entry::RaftEntry;
use crate::storage::IOFlushed;
use crate::storage::LogState;
use crate::storage::RaftLogStorage;
use crate::type_config::alias::EntryOf;
use crate::type_config::alias::LogIdOf;
use crate::type_config::alias::VoteOf;
use crate::type_config::alias::WatchReceiverOf;
use crate::type_config::alias::WatchSenderOf;

/// A log whose reads each find one entry, once `open` is `true`.
pub(super) struct GatedLog {
    /// Counts the reads that have started.
    pub(super) reads: Arc<WatchSenderOf<C, u64>>,
    pub(super) open: WatchReceiverOf<C, bool>,
}

impl RaftLogReader<C> for GatedLog {
    async fn limited_get_log_entries(&mut self, start: u64, _end: u64) -> Result<Vec<EntryOf<C>>, io::Error> {
        let reads = *self.reads.borrow_watched() + 1;
        self.reads.send(reads).ok();

        self.open.wait_until_ge(&true).await.ok();
        Ok(vec![EntryOf::<C>::new_blank(log_id(1, 1, start))])
    }

    async fn try_get_log_entries<R>(&mut self, _range: R) -> Result<Vec<EntryOf<C>>, io::Error>
    where R: RangeBounds<u64> + Clone + Debug + OptionalSend {
        unreachable!("replication should use limited_get_log_entries")
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, io::Error> {
        unreachable!()
    }
}

impl RaftLogStorage<C> for GatedLog {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
        unreachable!()
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        unreachable!()
    }

    async fn save_vote(&mut self, _vote: &VoteOf<C>) -> Result<(), io::Error> {
        unreachable!()
    }

    async fn append<I>(&mut self, _entries: I, _callback: IOFlushed<C>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<C>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        unreachable!()
    }

    async fn truncate_after(&mut self, _last_log_id: Option<LogIdOf<C>>) -> Result<(), io::Error> {
        unreachable!()
    }

    async fn purge(&mut self, _log_id: LogIdOf<C>) -> Result<(), io::Error> {
        unreachable!()
    }
}
