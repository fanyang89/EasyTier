// this mod wrap tunnel to a mpsc tunnel, based on crossbeam_channel

use std::{
    cell::UnsafeCell,
    pin::Pin,
    sync::Arc,
    sync::atomic::{AtomicBool, AtomicU32, Ordering},
    task::Poll,
    time::Duration,
};

use anyhow::Context;
use tokio::time::timeout;

use crate::proto::common::TunnelInfo;

use super::{Tunnel, TunnelError, ZCPacketSink, ZCPacketStream, packet_def::ZCPacket};

use tokio::sync::mpsc::{Receiver, Sender, channel, error::TrySendError};
use tokio_util::task::AbortOnDropHandle;

use futures::SinkExt;

const MPSC_TUNNEL_CHANNEL_SIZE: usize = 1024;
// Keep each timed forward round bounded even when producers never let rx become empty.
// Bound must stay small enough for one round to finish within `send_timeout`, so the
// forward task keeps making progress instead of timing out on a full batch.
const MPSC_TUNNEL_FORWARD_BATCH_SIZE: usize = 32;


/// A simple spinlock protecting a sink. The guard is Send because it only
/// contains an atomic flag reference (no lifetime-tied borrow like MutexGuard).
struct SpinSink {
    locked: AtomicBool,
    sink: UnsafeCell<Pin<Box<dyn ZCPacketSink>>>,
    pending_count: AtomicU32,
    batch_threshold: AtomicU32,
}

// SAFETY: access is serialized by the spinlock.
unsafe impl Send for SpinSink {}
unsafe impl Sync for SpinSink {}

struct SpinGuard<'a> {
    spin: &'a SpinSink,
}

impl<'a> SpinGuard<'a> {
    fn as_mut(&mut self) -> Pin<&mut dyn ZCPacketSink> {
        // SAFETY: we hold the spinlock, so we have exclusive access
        let sink = unsafe { &mut *self.spin.sink.get() };
        sink.as_mut()
    }
}

impl Drop for SpinGuard<'_> {
    fn drop(&mut self) {
        self.spin.locked.store(false, Ordering::Release);
    }
}

impl SpinSink {
    fn new(sink: Pin<Box<dyn ZCPacketSink>>) -> Self {
        Self {
            locked: AtomicBool::new(false),
            sink: UnsafeCell::new(sink),
            pending_count: AtomicU32::new(0),
            batch_threshold: AtomicU32::new(1),
        }
    }

    fn set_batch_threshold(&self, n: u32) {
        self.batch_threshold.store(n, Ordering::Relaxed);
    }

    fn try_lock(&self) -> Option<SpinGuard<'_>> {
        if self
            .locked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            Some(SpinGuard { spin: self })
        } else {
            None
        }
    }
}

#[derive(Clone)]
pub struct MpscTunnelSender {
    channel_tx: Option<Sender<ZCPacket>>,
    direct_sink: Option<Arc<SpinSink>>,
    direct_batch_flush: bool,
}

impl MpscTunnelSender {
    pub async fn send(&self, item: ZCPacket) -> Result<(), TunnelError> {
        if let Some(sink) = &self.direct_sink {
            // Sync fast path with cooperative backpressure. We poll the sink
            // with a noop waker so the common case (buffer has space) completes
            // with zero async machinery overhead. When the buffer is full,
            // poll_ready returns Pending under a noop waker that can never wake
            // us; instead of turning that into an error (which would drop
            // backpressure and cause BufferFull panics) or parking on a real
            // waker (whose wake latency is ~1000x a yield and dominates the
            // ring-full case), we yield cooperatively: the send task stays
            // runnable but lets the consumer (forward task / ring drain) take a
            // scheduling slot, then retries the fast path. This is real
            // backpressure — the producer makes no progress until the consumer
            // frees capacity — without the park/unpark tax.
            loop {
                if let Some(mut guard) = sink.try_lock() {
                    let waker = futures::task::noop_waker();
                    let mut cx = std::task::Context::from_waker(&waker);
                    match guard.as_mut().poll_ready(&mut cx) {
                        Poll::Ready(Ok(())) => {
                            guard.as_mut().start_send(item)?;
                            Self::batch_flush(sink, &mut guard, &mut cx)?;
                            return Ok(());
                        }
                        Poll::Ready(Err(e)) => return Err(e),
                        Poll::Pending => {
                            // Buffer full: yield so the consumer can drain,
                            // then retry the fast path.
                        }
                    }
                }
                // Either the spinlock is contended or the buffer is full:
                // yield to the runtime and retry.
                tokio::task::yield_now().await;
            }
        }

        // Channel mode: async with backpressure
        self.send_async(item).await
    }

    // Batch flush helper. pending_count tracks items accumulated since the last
    // flush; once it crosses the threshold we issue one poll_flush (writev for
    // FramedWriter, no-op for RingSink). poll_flush returning Pending is treated
    // as success: the data is already in the ring buffer / BufList and will be
    // consumed by the forward task or a later send (TCP send buffer full is not
    // an error, see bench/007 坑10). A Ready(Err) (real failure, e.g. closed
    // connection) is propagated to the caller.
    fn batch_flush(
        sink: &SpinSink,
        guard: &mut SpinGuard<'_>,
        cx: &mut std::task::Context<'_>,
    ) -> Result<(), TunnelError> {
        let count = sink.pending_count.fetch_add(1, Ordering::Relaxed) + 1;
        let threshold = sink.batch_threshold.load(Ordering::Relaxed);
        if count >= threshold {
            sink.pending_count.store(0, Ordering::Relaxed);
            if let Poll::Ready(Err(e)) = guard.as_mut().poll_flush(cx) {
                return Err(e);
            }
        }
        Ok(())
    }

    pub fn try_send(&self, item: ZCPacket) -> Result<(), TunnelError> {
        let tx = self.channel_tx.as_ref().ok_or(TunnelError::Shutdown)?;
        tx.try_send(item).map_err(|e| match e {
            TrySendError::Full(_) => TunnelError::BufferFull,
            TrySendError::Closed(_) => TunnelError::Shutdown,
        })
    }

    pub fn set_batch_threshold(&self, n: u32) {
        if let Some(sink) = &self.direct_sink {
            sink.set_batch_threshold(n);
        }
    }

    pub async fn send_async(&self, item: ZCPacket) -> Result<(), TunnelError> {
        let tx = self.channel_tx.as_ref().ok_or(TunnelError::Shutdown)?;
        match tx.try_send(item) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(item)) => {
                tx.send(item).await.with_context(|| "send error")?;
                Ok(())
            }
            Err(TrySendError::Closed(_)) => Err(TunnelError::Shutdown),
        }
    }
}

pub struct MpscTunnel<T> {
    tx: Option<Sender<ZCPacket>>,
    direct_sink: Option<Arc<SpinSink>>,
    direct_batch_flush: bool,

    tunnel: T,
    stream: Option<Pin<Box<dyn ZCPacketStream>>>,

    task: Option<AbortOnDropHandle<()>>,
}

impl<T: Tunnel> MpscTunnel<T> {
    pub fn new(tunnel: T, send_timeout: Option<Duration>) -> Self {
        let (tx, mut rx) = channel(MPSC_TUNNEL_CHANNEL_SIZE);
        let (stream, mut sink) = tunnel.split();

        let task = tokio::spawn(async move {
            loop {
                if let Err(e) = Self::forward_one_round(&mut rx, &mut sink, send_timeout).await {
                    tracing::error!(?e, "forward error");
                    break;
                }
            }
            rx.close();
            let close_ret = timeout(Duration::from_secs(5), sink.close()).await;
            tracing::warn!(?close_ret, "mpsc close sink");
        });

        Self {
            tx: Some(tx),
            direct_sink: None,
            direct_batch_flush: false,
            tunnel,
            stream: Some(stream),
            task: Some(AbortOnDropHandle::new(task)),
        }
    }

    pub fn new_direct(tunnel: T) -> Self {
        let (stream, sink) = tunnel.split();
        let info = tunnel.info();
        let batch_flush = info
            .as_ref()
            .map(|i| matches!(i.tunnel_type.as_str(), "ring" | "udp"))
            .unwrap_or(false);
        Self {
            tx: None,
            direct_sink: Some(Arc::new(SpinSink::new(sink))),
            direct_batch_flush: batch_flush,
            tunnel,
            stream: Some(stream),
            task: None,
        }
    }

    async fn forward_one_round(
        rx: &mut Receiver<ZCPacket>,
        sink: &mut Pin<Box<dyn ZCPacketSink>>,
        send_timeout_ms: Option<Duration>,
    ) -> Result<(), TunnelError> {
        let item = rx.recv().await.with_context(|| "recv error")?;
        if let Some(timeout_ms) = send_timeout_ms {
            Self::forward_one_round_with_timeout(rx, sink, item, timeout_ms).await
        } else {
            Self::forward_one_round_no_timeout(rx, sink, item).await
        }
    }

    async fn forward_one_round_no_timeout(
        rx: &mut Receiver<ZCPacket>,
        sink: &mut Pin<Box<dyn ZCPacketSink>>,
        initial_item: ZCPacket,
    ) -> Result<(), TunnelError> {
        sink.feed(initial_item).await?;

        for _ in 1..MPSC_TUNNEL_FORWARD_BATCH_SIZE {
            let Ok(item) = rx.try_recv() else {
                break;
            };
            if let Err(e) = sink.feed(item).await {
                tracing::error!(?e, "feed error");
                return Err(e);
            }
        }

        sink.flush().await
    }

    async fn forward_one_round_with_timeout(
        rx: &mut Receiver<ZCPacket>,
        sink: &mut Pin<Box<dyn ZCPacketSink>>,
        initial_item: ZCPacket,
        timeout_ms: Duration,
    ) -> Result<(), TunnelError> {
        match timeout(timeout_ms, async move {
            Self::forward_one_round_no_timeout(rx, sink, initial_item).await
        })
        .await
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => {
                tracing::error!(?e, "forward error");
                Err(e)
            }
            Err(e) => {
                tracing::error!(?e, "forward timeout");
                Err(e.into())
            }
        }
    }

    pub fn get_stream(&mut self) -> Pin<Box<dyn ZCPacketStream>> {
        self.stream.take().unwrap()
    }

    pub fn get_sink(&self) -> MpscTunnelSender {
        MpscTunnelSender {
            channel_tx: self.tx.as_ref().cloned(),
            direct_sink: self.direct_sink.clone(),
            direct_batch_flush: self.direct_batch_flush,
        }
    }

    pub fn close(&mut self) {
        self.tx.take();
        self.direct_sink.take();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }

    pub fn tunnel_info(&self) -> Option<TunnelInfo> {
        self.tunnel.info()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
    };

    use futures::{Sink, StreamExt};
    use tokio::task::JoinSet;

    use crate::tunnel::{
        SinkItem, StreamItem, TunnelConnector, TunnelListener,
        common::TunnelWrapper,
        ring::{RING_TUNNEL_CAP, create_ring_tunnel_pair},
        tcp::{TcpTunnelConnector, TcpTunnelListener},
    };

    use super::*;

    struct ProgressSink {
        delay: Duration,
        sleep: Pin<Box<tokio::time::Sleep>>,
        waiting: bool,
        sent: Arc<AtomicUsize>,
    }

    impl ProgressSink {
        fn new(delay: Duration, sent: Arc<AtomicUsize>) -> Self {
            Self {
                delay,
                sleep: Box::pin(tokio::time::sleep(Duration::ZERO)),
                waiting: false,
                sent,
            }
        }
    }

    impl Sink<SinkItem> for ProgressSink {
        type Error = TunnelError;

        fn poll_ready(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if !self.waiting {
                return Poll::Ready(Ok(()));
            }

            match self.sleep.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    self.waiting = false;
                    Poll::Ready(Ok(()))
                }
                Poll::Pending => Poll::Pending,
            }
        }

        fn start_send(mut self: Pin<&mut Self>, _item: SinkItem) -> Result<(), Self::Error> {
            let wake_at = tokio::time::Instant::now() + self.delay;
            self.sleep.as_mut().reset(wake_at);
            self.waiting = true;
            self.sent.fetch_add(1, Ordering::Release);
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn mpsc_continuous_progress_does_not_timeout() {
        let sent = Arc::new(AtomicUsize::new(0));
        let tunnel = TunnelWrapper::new(
            futures::stream::pending::<StreamItem>(),
            ProgressSink::new(Duration::from_millis(1), sent.clone()),
            None,
        );
        let mpsc_tunnel = MpscTunnel::new(tunnel, Some(Duration::from_millis(200)));
        let sink = mpsc_tunnel.get_sink();

        let producer_count = 4;
        let packets_per_producer = 256;
        let total_packets = producer_count * packets_per_producer;
        let mut tasks = JoinSet::new();
        for _ in 0..producer_count {
            let sink = sink.clone();
            tasks.spawn(async move {
                for _ in 0..packets_per_producer {
                    sink.send(ZCPacket::new_with_payload(&[0; 64])).await?;
                }
                Ok::<(), TunnelError>(())
            });
        }

        while let Some(ret) = tasks.join_next().await {
            ret.expect("producer task panicked")
                .expect("producer send failed");
        }

        tokio::time::timeout(Duration::from_secs(10), async {
            while sent.load(Ordering::Acquire) < total_packets {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("forward task stopped while the sink was making progress");
    }

    // Reproduces the direct sink path backpressure: fill the ring (no consumer),
    // then send must park on the slow path and be woken once a consumer drains
    // the server stream. Validates the noop_waker fast path -> poll_fn real
    // waker slow path transition.
    #[tokio::test]
    async fn mpsc_direct_sink_backpressure_wakes_after_drain() {
        let (server_tun, client_tun) = create_ring_tunnel_pair();
        let client = MpscTunnel::new_direct(client_tun);
        let sink = client.get_sink();

        // Fill the ring with no consumer. The first RING_TUNNEL_CAP sends land
        // in the ring buffer; subsequent sends must exercise the slow path.
        for i in 0..RING_TUNNEL_CAP {
            sink.send(ZCPacket::new_with_payload(&[i as u8; 64]))
                .await
                .expect("fill send must succeed");
        }

        // Spawn a consumer that drains the server stream after 100ms. The ring
        // is full at this point, so the next send must park and be woken.
        let _drain = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let (mut stream, _sink) = server_tun.split();
            loop {
                match tokio::time::timeout(Duration::from_millis(50), stream.next()).await {
                    Ok(Some(_)) => continue,
                    _ => break,
                }
            }
        });

        // This send must complete once the consumer drains the ring.
        tokio::time::timeout(
            Duration::from_secs(3),
            sink.send(ZCPacket::new_with_payload(&[0x42; 64])),
        )
        .await
        .expect("direct sink send was not woken after ring drain")
        .expect("send returned error");
    }

    // test slow send lock in framed tunnel
    #[tokio::test]
    async fn mpsc_slow_receiver() {
        let mut listener = TcpTunnelListener::new("tcp://127.0.0.1:11014".parse().unwrap());
        let mut connector = TcpTunnelConnector::new("tcp://127.0.0.1:11014".parse().unwrap());

        listener.listen().await.unwrap();
        let t1 = tokio::spawn(async move {
            let t = listener.accept().await.unwrap();
            let (mut stream, _sink) = t.split();
            let now = tokio::time::Instant::now();

            let mut a_counter = 0;
            let mut b_counter = 0;

            while let Some(Ok(msg)) = stream.next().await {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                if now.elapsed().as_secs() > 5 {
                    break;
                }

                if msg.payload() == "hello".as_bytes() {
                    a_counter += 1;
                } else if msg.payload() == "hello2".as_bytes() {
                    b_counter += 1;
                }
            }

            tracing::info!("t1 exit");
            assert_ne!(a_counter, 0);
            assert_ne!(b_counter, 0);
        });

        let tunnel = connector.connect().await.unwrap();
        let mpsc_tunnel = MpscTunnel::new(tunnel, None);

        let sink1 = mpsc_tunnel.get_sink();
        let t2 = tokio::spawn(async move {
            for i in 0..1000000 {
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                let a = sink1
                    .send_async(ZCPacket::new_with_payload("hello".as_bytes())).await;
                if a.is_err() {
                    tracing::info!(?a, "t2 exit with err");
                    break;
                }

                if i % 5000 == 0 {
                    tracing::info!(i, "send2 1000");
                }
            }

            tracing::info!("t2 exit");
        });

        let sink2 = mpsc_tunnel.get_sink();
        let t3 = tokio::spawn(async move {
            for i in 0..1000000 {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                let a = sink2
                    .send_async(ZCPacket::new_with_payload("hello2".as_bytes())).await;
                if a.is_err() {
                    tracing::info!(?a, "t3 exit with err");
                    break;
                }

                if i % 5000 == 0 {
                    tracing::info!(i, "send2 1000");
                }
            }

            tracing::info!("t3 exit");
        });

        let t4 = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            tracing::info!("closing");
            drop(mpsc_tunnel);
            tracing::info!("closed");
        });

        let _ = tokio::join!(t1, t2, t3, t4);
    }

    #[tokio::test]
    async fn mpsc_slow_receiver_with_send_timeout() {
        let (a, _b) = create_ring_tunnel_pair();
        let mpsc_tunnel = MpscTunnel::new(a, Some(Duration::from_secs(1)));
        let s = mpsc_tunnel.get_sink();
        for _ in 0..RING_TUNNEL_CAP {
            s.send(ZCPacket::new_with_payload(&[0; 1024]))
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let e = s.send(ZCPacket::new_with_payload(&[0; 1024])).await;
        assert!(e.is_ok());

        tokio::time::sleep(Duration::from_millis(1500)).await;

        let e = s.send(ZCPacket::new_with_payload(&[0; 1024])).await;
        assert!(e.is_err());
    }
}
