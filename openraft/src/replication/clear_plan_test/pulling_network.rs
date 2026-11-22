use std::future::Future;
use std::sync::Arc;

use futures_util::Stream;
use futures_util::StreamExt;

use super::C;
use crate::OptionalSend;
use crate::RaftNetworkFactory;
use crate::async_runtime::WatchReceiver;
use crate::async_runtime::WatchSender;
use crate::base::BoxFuture;
use crate::base::BoxStream;
use crate::errors::RPCError;
use crate::errors::ReplicationClosed;
use crate::errors::StreamingError;
use crate::errors::Unreachable;
use crate::network::RPCOption;
use crate::network::RaftNetworkV2;
use crate::raft::AppendEntriesRequest;
use crate::raft::AppendResult;
use crate::raft::SnapshotResponse;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::SnapshotOf;
use crate::type_config::alias::VoteOf;
use crate::type_config::alias::WatchReceiverOf;
use crate::type_config::alias::WatchSenderOf;

/// A transport that polls the request stream on a task of its own, as a gRPC client does, and
/// answers no request.
pub(super) struct PullingNetwork {
    /// The polling task starts once this is `true`.
    pub(super) poll: WatchReceiverOf<C, bool>,
    /// The number of requests the polling task has taken from the stream.
    pub(super) pulled: Arc<WatchSenderOf<C, u64>>,
    /// Turns `true` once the request stream has ended.
    pub(super) ended: Arc<WatchSenderOf<C, bool>>,
    /// Whether a session fails at once with an RPC error. Otherwise its response stream ends after
    /// its request stream does.
    pub(super) fail: bool,
}

impl RaftNetworkFactory<C> for PullingNetwork {
    type Network = Self;

    async fn new_client(&mut self, _target: u64, _node: &()) -> Self {
        unreachable!("the test hands the network to the replication task")
    }
}

impl RaftNetworkV2<C> for PullingNetwork {
    type SnapshotData = ();

    fn stream_append<'s, S>(
        &'s mut self,
        mut input: S,
        _option: RPCOption,
    ) -> BoxFuture<'s, Result<BoxStream<'s, Result<AppendResult<C>, RPCError<C>>>, RPCError<C>>>
    where
        S: Stream<Item = AppendEntriesRequest<C>> + OptionalSend + Unpin + 'static,
    {
        let mut poll = self.poll.clone();
        let pulled = self.pulled.clone();
        let ended = self.ended.clone();
        #[allow(clippy::let_underscore_future)]
        let _ = C::spawn(async move {
            if poll.wait_until_ge(&true).await.is_err() {
                return;
            }

            while input.next().await.is_some() {
                let count = *pulled.borrow_watched() + 1;
                pulled.send(count).ok();
            }
            ended.send(true).ok();
        });

        let responses: BoxStream<'s, Result<AppendResult<C>, RPCError<C>>> = if self.fail {
            let error = RPCError::Unreachable(Unreachable::from_string("node 2 is down"));
            Box::pin(futures_util::stream::once(futures_util::future::ready(Err(error))))
        } else {
            let mut ended = self.ended.subscribe();
            let end = async move {
                ended.wait_until_ge(&true).await.ok();
            };
            Box::pin(futures_util::stream::once(end).filter_map(|()| futures_util::future::ready(None)))
        };
        Box::pin(futures_util::future::ready(Ok(responses)))
    }

    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendResult<C>, RPCError<C>> {
        unreachable!("replication streams its requests")
    }

    async fn vote(&mut self, _rpc: VoteRequest<C>, _option: RPCOption) -> Result<VoteResponse<C>, RPCError<C>> {
        unreachable!()
    }

    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<C>,
        _snapshot: SnapshotOf<C, ()>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        unreachable!()
    }
}
