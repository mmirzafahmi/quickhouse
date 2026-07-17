//! Process-wide memory budget for in-flight Arrow batches.
//!
//! The transfer pipeline decodes batches on one side and uploads them to
//! ClickHouse concurrently on the other. Without a ceiling, a fast source and
//! a slow sink would let decoded-but-not-yet-sent batches pile up unbounded.
//!
//! [`MemoryBudget`] is a byte-denominated semaphore (1 permit = 1 byte): a
//! batch reserves permits equal to its real [`RecordBatch::get_array_memory_size`]
//! before it may be handed to a send task, and releases them when the send
//! completes. This makes peak in-flight memory a single honest number
//! (`max_memory_bytes`) that holds regardless of parallelism, row width, or
//! partition skew — unlike a per-batch row/byte heuristic, whose true peak is
//! an emergent `parallelism × batch_size`.
//!
//! [`RecordBatch::get_array_memory_size`]: arrow_array::RecordBatch::get_array_memory_size

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// tokio's `Semaphore` caps total permits at `usize::MAX >> 3`, and
/// `acquire_many_owned` takes a `u32`. Batches are MiB-scale so a single
/// reservation never approaches this, but clamp defensively so an absurd
/// size can never panic or wrap.
const MAX_PERMITS: u32 = u32::MAX;

/// A shared ceiling on total in-flight Arrow batch memory.
///
/// Cloneable and cheap to share across tasks (an `Arc` inside). `None`-like
/// "unbounded" is represented by constructing with `max_bytes == 0`.
#[derive(Clone)]
pub struct MemoryBudget {
    /// `None` = unbounded (no accounting at all).
    sem: Option<Arc<Semaphore>>,
    /// The configured ceiling, retained so a single oversized batch can be
    /// clamped to "reserve the whole budget" rather than deadlock.
    total: usize,
}

impl MemoryBudget {
    /// Create a budget of `max_bytes`. `0` means unbounded.
    pub fn new(max_bytes: usize) -> Self {
        let sem = if max_bytes == 0 {
            None
        } else {
            // Semaphore permit count is a usize; cap at the tokio maximum.
            let permits = max_bytes.min(Semaphore::MAX_PERMITS);
            Some(Arc::new(Semaphore::new(permits)))
        };
        MemoryBudget {
            sem,
            total: max_bytes,
        }
    }

    /// Whether this budget actually enforces a ceiling.
    pub fn is_bounded(&self) -> bool {
        self.sem.is_some()
    }

    /// Reserve room for a batch of `size` bytes, awaiting until enough is free.
    ///
    /// Returns a permit whose `Drop` releases the reservation — hold it for as
    /// long as the batch occupies memory (i.e. until its upload finishes).
    /// A batch larger than the whole budget is clamped to the full budget so
    /// it still runs (alone) rather than deadlocking forever.
    pub async fn reserve(&self, size: usize) -> Reservation {
        match &self.sem {
            None => Reservation { _permit: None },
            Some(sem) => {
                let want = size.min(self.total).min(MAX_PERMITS as usize).max(1) as u32;
                // acquire_many_owned only errors if the semaphore is closed,
                // which we never do; unwrap is safe for the lifetime of self.
                let permit = sem
                    .clone()
                    .acquire_many_owned(want)
                    .await
                    .expect("memory budget semaphore closed unexpectedly");
                Reservation {
                    _permit: Some(permit),
                }
            }
        }
    }
}

/// An outstanding memory reservation. Dropping it returns the bytes to the
/// budget. Opaque on purpose — callers only need to keep it alive.
pub struct Reservation {
    _permit: Option<OwnedSemaphorePermit>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unbounded_never_blocks() {
        let budget = MemoryBudget::new(0);
        assert!(!budget.is_bounded());
        // Many large reservations resolve immediately and independently.
        let _a = budget.reserve(10_000_000_000).await;
        let _b = budget.reserve(10_000_000_000).await;
    }

    #[tokio::test]
    async fn releases_on_drop() {
        let budget = MemoryBudget::new(1000);
        {
            let _r = budget.reserve(1000).await; // takes the whole budget
                                                 // A second full reservation would block here; instead we drop first.
        }
        // After drop, the full budget is available again — this must not hang.
        let _r2 = budget.reserve(1000).await;
    }

    #[tokio::test]
    async fn oversized_batch_still_proceeds() {
        // A batch bigger than the entire budget must not deadlock; it is
        // clamped to the whole budget and runs alone.
        let budget = MemoryBudget::new(1000);
        let _r = budget.reserve(5000).await; // clamped to 1000, resolves
    }

    #[tokio::test]
    async fn blocks_until_released() {
        use std::time::Duration;
        use tokio::time::timeout;

        let budget = MemoryBudget::new(1000);
        let first = budget.reserve(1000).await; // exhausts the budget

        // A second reservation cannot complete while `first` is held.
        let second = budget.reserve(1000);
        assert!(
            timeout(Duration::from_millis(50), second).await.is_err(),
            "reservation should block while the budget is fully held"
        );

        // Releasing the first frees the budget for a waiter.
        drop(first);
        let _third = timeout(Duration::from_millis(500), budget.reserve(1000))
            .await
            .expect("reservation should proceed after the budget is released");
    }
}
