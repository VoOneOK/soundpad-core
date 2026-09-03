use ringbuf::{HeapRb, SharedRb, storage::Heap, traits::*, wrap::caching::Caching};
use std::sync::Arc;

pub fn create_ring_buffer<T>(
    capacity: usize,
) -> (
    Caching<Arc<SharedRb<Heap<T>>>, true, false>,
    Caching<Arc<SharedRb<Heap<T>>>, false, true>,
) {
    let ring_buffer = HeapRb::<T>::new(capacity);
    ring_buffer.split()
}
