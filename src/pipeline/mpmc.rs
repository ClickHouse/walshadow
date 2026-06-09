//! Bounded multi-producer multi-consumer channel on tokio primitives.
//!
//! tokio's `mpsc` is single-consumer; the pipeline fans one queue to N
//! inserters and M decoders. Rather than pull in `async-channel`, wrap a
//! `Mutex<VecDeque>` between two `Semaphore`s: `items` counts queued elements
//! (recv waits), `space` counts free slots (send waits). Both semaphores are
//! multi-waiter and FIFO-fair. `close` makes pending + future `acquire`s
//! fail, draining whatever is queued before recv yields `None`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Semaphore;

struct Shared<T> {
    q: Mutex<VecDeque<T>>,
    items: Semaphore,
    space: Semaphore,
    senders: AtomicUsize,
}

pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // Last sender gone: close item side so `recv`s drain then yield `None`
        // (graceful shutdown by dropping senders, like tokio's mpsc)
        if self.shared.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.items.close();
        }
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

/// Bounded mpmc with room for `capacity` in-flight items (min 1).
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        q: Mutex::new(VecDeque::with_capacity(capacity.max(1))),
        items: Semaphore::new(0),
        space: Semaphore::new(capacity.max(1)),
        senders: AtomicUsize::new(1),
    });
    (
        Sender {
            shared: shared.clone(),
        },
        Receiver { shared },
    )
}

impl<T> Sender<T> {
    /// Await a free slot, then enqueue. `Err(item)` returns the item when the
    /// channel was closed.
    pub async fn send(&self, item: T) -> Result<(), T> {
        match self.shared.space.acquire().await {
            Ok(p) => p.forget(),
            Err(_) => return Err(item),
        }
        self.shared
            .q
            .lock()
            .expect("mpmc queue poisoned")
            .push_back(item);
        self.shared.items.add_permits(1);
        Ok(())
    }

    /// Close both ends; queued items still drain via `recv`.
    pub fn close(&self) {
        self.shared.items.close();
        self.shared.space.close();
    }
}

impl<T> Receiver<T> {
    /// Await an item. `None` once the channel is closed and drained.
    pub async fn recv(&self) -> Option<T> {
        match self.shared.items.acquire().await {
            Ok(p) => {
                p.forget();
                let item = self
                    .shared
                    .q
                    .lock()
                    .expect("mpmc queue poisoned")
                    .pop_front();
                self.shared.space.add_permits(1);
                item
            }
            Err(_) => self
                .shared
                .q
                .lock()
                .expect("mpmc queue poisoned")
                .pop_front(),
        }
    }

    pub fn close(&self) {
        self.shared.items.close();
        self.shared.space.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[tokio::test]
    async fn spsc_preserves_order() {
        let (tx, rx) = channel::<u64>(2);
        let producer = tokio::spawn(async move {
            for i in 0..50 {
                tx.send(i).await.expect("send");
            }
            tx.close();
        });
        let mut got = Vec::new();
        while let Some(v) = rx.recv().await {
            got.push(v);
        }
        producer.await.unwrap();
        assert_eq!(got, (0..50).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn mpmc_delivers_every_item_once() {
        let (tx, rx) = channel::<u64>(4);
        let total = Arc::new(AtomicU64::new(0));
        let count = Arc::new(AtomicU64::new(0));
        let mut producers = Vec::new();
        for p in 0..3u64 {
            let tx = tx.clone();
            producers.push(tokio::spawn(async move {
                for i in 0..100u64 {
                    tx.send(p * 1000 + i).await.expect("send");
                }
            }));
        }
        let mut consumers = Vec::new();
        for _ in 0..4 {
            let rx = rx.clone();
            let total = total.clone();
            let count = count.clone();
            consumers.push(tokio::spawn(async move {
                while let Some(v) = rx.recv().await {
                    total.fetch_add(v, Ordering::Relaxed);
                    count.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for p in producers {
            p.await.unwrap();
        }
        tx.close();
        for c in consumers {
            c.await.unwrap();
        }
        assert_eq!(count.load(Ordering::Relaxed), 300);
        let expected: u64 = (0..3u64)
            .flat_map(|p| (0..100u64).map(move |i| p * 1000 + i))
            .sum();
        assert_eq!(total.load(Ordering::Relaxed), expected);
    }

    #[tokio::test]
    async fn close_drains_then_none() {
        let (tx, rx) = channel::<u64>(8);
        for i in 0..5 {
            tx.send(i).await.expect("send");
        }
        tx.close();
        let mut got = Vec::new();
        while let Some(v) = rx.recv().await {
            got.push(v);
        }
        assert_eq!(got, vec![0, 1, 2, 3, 4]);
        assert!(rx.recv().await.is_none());
    }
}
