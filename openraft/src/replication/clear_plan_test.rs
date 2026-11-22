//! A replication task confirms a clear only once it reads nothing more for the plan RaftCore
//! dropped. RaftCore purges the logs the plan held back only after the confirmation.

mod gated_log;
mod harness;
mod pulling_network;

use std::time::Duration;

use harness::Harness;

use crate::async_runtime::MpscReceiver;
use crate::async_runtime::WatchReceiver;
use crate::async_runtime::WatchSender;
use crate::core::notification::Notification;
use crate::engine::testing::UTConfig;
use crate::engine::testing::log_id;
use crate::log_id_range::LogIdRange;
use crate::progress::inflight_id::InflightId;
use crate::replication::replicate::Replicate;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::MpscReceiverOf;

type C = UTConfig;

const TEST_TIMEOUT: Duration = Duration::from_secs(1);

/// Wait for the next notification, which must be the confirmation of the clear `inflight_id`.
async fn expect_cleared(notifications: &mut MpscReceiverOf<C, Notification<C>>, inflight_id: InflightId) {
    let notification = C::timeout(TEST_TIMEOUT, notifications.recv()).await.unwrap().unwrap();
    let cleared = match &notification {
        Notification::ReplicateCleared { inflight_id: id, .. } => *id == inflight_id,
        _ => false,
    };
    assert!(
        cleared,
        "expect the clear {} confirmed, got: {}",
        inflight_id, notification
    );
}

/// Wait for the next notification, which must be the RPC error that ended the session of the plan
/// `inflight_id`.
async fn expect_failed(notifications: &mut MpscReceiverOf<C, Notification<C>>, inflight_id: InflightId) {
    let notification = C::timeout(TEST_TIMEOUT, notifications.recv()).await.unwrap().unwrap();
    let failed = match &notification {
        Notification::ReplicationProgress {
            progress,
            inflight_id: id,
            ..
        } => progress.result.is_err() && *id == Some(inflight_id),
        _ => false,
    };
    assert!(
        failed,
        "expect the RPC error of plan {}, got: {}",
        inflight_id, notification
    );
}

/// A read for a plan that RaftCore clears may still be running when the clear arrives. The
/// replication task confirms the clear only once the read has returned and the stream session has
/// ended, and it sends nothing the read returned.
#[test]
fn clear_is_confirmed_once_a_running_read_returns() {
    C::run(async {
        let (mut h, _channels) = Harness::spawn(false, true, false);
        let plan = InflightId::new(1);
        let clear = InflightId::new(2);

        tracing::info!("--- a plan for the logs after index 5 starts a read, which the log holds");
        {
            h.replicate_tx.send(Some(Replicate::new_logs_since(Some(log_id(1, 1, 5)), plan))).unwrap();
            C::timeout(TEST_TIMEOUT, h.reads.wait_until_ge(&1)).await.unwrap().unwrap();
        }

        tracing::info!("--- RaftCore clears the plan: no confirmation while the read runs");
        {
            h.replicate_tx.send(Some(Replicate::new_clear(clear))).unwrap();

            C::sleep(Duration::from_millis(100)).await;
            let notification = h.notifications.try_recv();
            assert!(notification.is_err(), "expect no notification while the read runs");
        }

        tracing::info!("--- the read returns: the stream sends nothing, and the task confirms the clear");
        {
            h.open_reads.send(true).unwrap();
            expect_cleared(&mut h.notifications, clear).await;

            C::timeout(TEST_TIMEOUT, h.ended.wait_until_ge(&true)).await.unwrap().unwrap();
            assert_eq!(0, *h.pulled.borrow_watched(), "the stream ends without a request");
            assert_eq!(1, *h.reads.borrow_watched());
        }
    });
}

/// After a session fails, a transport that polls the request stream on a task of its own may still
/// start a read for the plan while the replication task waits for RaftCore. The task confirms the
/// clear that follows only once that read returns.
#[test]
fn clear_is_confirmed_once_a_read_that_outlived_its_session_returns() {
    C::run(async {
        let (mut h, _channels) = Harness::spawn(false, false, true);
        let plan = InflightId::new(1);
        let clear = InflightId::new(2);

        tracing::info!("--- a plan for the logs after index 5 starts a session, which fails");
        {
            h.replicate_tx.send(Some(Replicate::new_logs_since(Some(log_id(1, 1, 5)), plan))).unwrap();
            expect_failed(&mut h.notifications, plan).await;
        }

        tracing::info!(
            "--- with the task waiting for RaftCore, the transport polls the stream, whose read the log holds"
        );
        {
            // Let the task go back to waiting for RaftCore after the error, so that the read starts
            // while it waits.
            C::sleep(Duration::from_millis(50)).await;
            h.poll.send(true).unwrap();
            C::timeout(TEST_TIMEOUT, h.reads.wait_until_ge(&1)).await.unwrap().unwrap();
        }

        tracing::info!("--- RaftCore clears the plan: no confirmation while the read runs");
        {
            h.replicate_tx.send(Some(Replicate::new_clear(clear))).unwrap();

            C::sleep(Duration::from_millis(100)).await;
            let notification = h.notifications.try_recv();
            assert!(notification.is_err(), "expect no notification while the read runs");
        }

        tracing::info!("--- the read returns: the stream sends nothing, and the task confirms the clear");
        {
            h.open_reads.send(true).unwrap();
            expect_cleared(&mut h.notifications, clear).await;

            C::timeout(TEST_TIMEOUT, h.ended.wait_until_ge(&true)).await.unwrap().unwrap();
            assert_eq!(0, *h.pulled.borrow_watched(), "the stream ends without a request");
            assert_eq!(1, *h.reads.borrow_watched());
        }
    });
}

/// Once the replication task confirms a clear, the request stream of the cleared plan reads
/// nothing, also when the transport polls it only after the session ended. The plan here is a
/// probe, for which the stream does not check RaftCore's current plan before it reads.
#[test]
fn stream_reads_nothing_once_the_clear_is_confirmed() {
    C::run(async {
        let (mut h, _channels) = Harness::spawn(true, false, true);
        let plan = InflightId::new(1);
        let clear = InflightId::new(2);

        tracing::info!(
            "--- a probe of the entries after index 5 starts a session, which fails before the transport polls"
        );
        {
            let probe = LogIdRange::new(Some(log_id(1, 1, 5)), Some(log_id(1, 1, 9)));
            h.replicate_tx.send(Some(Replicate::new_probe(probe, plan))).unwrap();
            expect_failed(&mut h.notifications, plan).await;
        }

        tracing::info!("--- RaftCore clears the plan, and the task confirms it");
        {
            h.replicate_tx.send(Some(Replicate::new_clear(clear))).unwrap();
            expect_cleared(&mut h.notifications, clear).await;
        }

        tracing::info!("--- the transport polls the stream only now: it ends without a read");
        {
            h.poll.send(true).unwrap();

            C::timeout(TEST_TIMEOUT, h.ended.wait_until_ge(&true)).await.unwrap().unwrap();
            assert_eq!(0, *h.pulled.borrow_watched(), "the stream ends without a request");
            assert_eq!(0, *h.reads.borrow_watched());
        }
    });
}
