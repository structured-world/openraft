use std::sync::Arc;

use super::C;
use super::gated_log::GatedLog;
use super::pulling_network::PullingNetwork;
use crate::Config;
use crate::core::SharedReplicateBatch;
use crate::core::notification::Notification;
use crate::engine::testing::log_id;
use crate::progress::stream_id::StreamId;
use crate::raft_state::IOId;
use crate::replication::ReplicationCore;
use crate::replication::event_watcher::EventWatcher;
use crate::replication::replicate::Replicate;
use crate::replication::replication_context::ReplicationContext;
use crate::replication::replication_progress::ReplicationProgress;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::MpscReceiverOf;
use crate::type_config::alias::VoteOf;
use crate::type_config::alias::WatchReceiverOf;
use crate::type_config::alias::WatchSenderOf;
use crate::vote::raft_vote::RaftVoteExt;

/// The channels a test drives and watches a replication task to node 2 through.
pub(super) struct Harness {
    pub(super) replicate_tx: WatchSenderOf<C, Option<Replicate<C>>>,
    pub(super) notifications: MpscReceiverOf<C, Notification<C>>,
    pub(super) reads: WatchReceiverOf<C, u64>,
    pub(super) open_reads: WatchSenderOf<C, bool>,
    pub(super) poll: WatchSenderOf<C, bool>,
    pub(super) pulled: WatchReceiverOf<C, u64>,
    pub(super) ended: WatchReceiverOf<C, bool>,
}

impl Harness {
    /// Spawn a replication task to node 2, whose log is submitted through index 20 and matches it
    /// through index 5. `open_reads` lets log reads return at once, `poll` lets the transport poll
    /// the request stream at once, and `fail` makes each session fail, see [`PullingNetwork`].
    /// Next to the harness, it returns a value that keeps the other channels of the task open.
    pub(super) fn spawn(open_reads: bool, poll: bool, fail: bool) -> (Self, impl Sized) {
        let vote = VoteOf::<C>::new_committed(1, 1).to_committed();
        let (replicate_tx, replicate_rx) = C::watch_channel(None);
        let (committed_tx, committed_rx) = C::watch_channel(None);
        let submitted = IOId::new_log_io(vote.clone(), Some(log_id(1, 1, 20)));
        let (io_tx, io_rx) = C::watch_channel(submitted);
        let (cancel_tx, cancel_rx) = C::watch_channel(());
        let (backoff_reset_tx, backoff_reset_rx) = C::watch_channel(());
        let (probe_tx, quorum_loss_probe_rx) = C::watch_channel(None);
        let (tx_notify, notifications) = C::mpsc(16);
        let (reads_tx, reads) = C::watch_channel(0);
        let (open_reads_tx, open_reads_rx) = C::watch_channel(open_reads);
        let (poll_tx, poll_rx) = C::watch_channel(poll);
        let (pulled_tx, pulled) = C::watch_channel(0);
        let (ended_tx, ended) = C::watch_channel(false);

        let context = ReplicationContext {
            id: 1,
            target: 2,
            leader_vote: vote,
            stream_id: StreamId::new(1),
            config: Arc::new(Config::default()),
            tx_notify,
            cancel_rx,
            replicate_batch: SharedReplicateBatch::new(),
            quorum_loss_probe_rx,
        };
        let event_watcher = EventWatcher {
            replicate_rx,
            committed_rx,
            io_accepted_rx: io_rx.clone(),
            io_submitted_rx: io_rx,
        };
        let progress = ReplicationProgress {
            local_committed: None,
            remote_matched: Some(log_id(1, 1, 5)),
        };
        let network = PullingNetwork {
            poll: poll_rx,
            pulled: Arc::new(pulled_tx),
            ended: Arc::new(ended_tx),
            fail,
        };
        let log = GatedLog {
            reads: Arc::new(reads_tx),
            open: open_reads_rx,
        };

        #[allow(clippy::let_underscore_future)]
        let _ = ReplicationCore::<C, PullingNetwork, GatedLog>::spawn(
            context,
            progress,
            network,
            log,
            event_watcher,
            backoff_reset_rx,
            tracing::Span::none(),
        );

        let harness = Harness {
            replicate_tx,
            notifications,
            reads,
            open_reads: open_reads_tx,
            poll: poll_tx,
            pulled,
            ended,
        };
        (harness, (committed_tx, io_tx, cancel_tx, backoff_reset_tx, probe_tx))
    }
}
