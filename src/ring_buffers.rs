use ringbuf::{HeapRb, SharedRb, storage::Heap, traits::*, wrap::caching::Caching};
use std::sync::Arc;

pub type RBConsumer<T> = Caching<Arc<SharedRb<Heap<T>>>, false, true>;
pub type RBProducer<T> = Caching<Arc<SharedRb<Heap<T>>>, true, false>;

pub fn create_ring_buffer<T>(capacity: usize) -> (RBProducer<T>, RBConsumer<T>) {
    let ring_buffer = HeapRb::<T>::new(capacity);
    ring_buffer.split()
}
