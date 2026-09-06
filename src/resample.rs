use audioadapter_buffers::direct::InterleavedSlice;
use core::f32;
use ringbuf::{
    SharedRb,
    storage::Heap,
    traits::{Consumer, Producer},
    wrap::caching::Caching,
};
use rubato::{Async, FixedAsync, Indexing, PolynomialDegree, Resampler};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

type InputConsumer = Caching<Arc<SharedRb<Heap<f32>>>, false, true>;
type OutputProducer = Caching<Arc<SharedRb<Heap<f32>>>, true, false>;

pub struct ResampleConfig {
    pub input_channels: usize,
    pub output_channels: usize,
    pub chunk_size: usize,
    pub ratio: f64,
    pub start_delay: u64,
    pub empty_buffer_retry_delay: u64,
}

pub fn start_resampling_loop(
    config: ResampleConfig,
    mut input_consumer: InputConsumer,
    mut output_producer: OutputProducer,
    keep_resampling: Arc<AtomicBool>,
) {
    let mut resampler = Async::<f32>::new_poly(
        config.ratio,
        1.1,
        PolynomialDegree::Cubic,
        config.chunk_size,
        config.input_channels,
        FixedAsync::Input,
    )
    .unwrap();

    let mut frames_to_read = resampler.input_frames_next();
    let mut samples_to_read = frames_to_read * config.input_channels;
    let mut indata: Vec<f32> = Vec::with_capacity(samples_to_read);
    let mut outdata = vec![0.0; config.input_channels * resampler.output_frames_max()];
    let outdata_capacity = outdata.len() / config.input_channels;

    let indexing = Indexing::new();

    thread::sleep(Duration::from_millis(config.start_delay));
    while keep_resampling.load(Ordering::Relaxed) {
        match input_consumer.try_pop() {
            Some(sample) => {
                indata.push(sample);
            }
            None => {
                thread::sleep(Duration::from_millis(config.empty_buffer_retry_delay));
                continue;
            }
        }

        if indata.len() < samples_to_read {
            continue;
        }

        let input_adapter =
            InterleavedSlice::new(&indata, config.input_channels, frames_to_read).unwrap();
        let mut output_adapter =
            InterleavedSlice::new_mut(&mut outdata, config.input_channels, outdata_capacity)
                .unwrap();

        let (_, frames_written) = resampler
            .process_into_buffer(&input_adapter, &mut output_adapter, Some(&indexing))
            .unwrap();

        indata.clear();

        frames_to_read = resampler.input_frames_next();
        samples_to_read = frames_to_read * config.input_channels;

        let samples_written = frames_written * config.input_channels;

        if config.input_channels == config.output_channels {
            for i in 0..samples_written {
                let _ = output_producer.try_push(outdata[i]);
            }
        } else if config.input_channels == 1 && config.output_channels == 2 {
            for i in 0..samples_written {
                let _ = output_producer.try_push(outdata[i]);
                let _ = output_producer.try_push(outdata[i]);
            }
        } else if config.input_channels == 2 && config.output_channels == 1 {
            for sample_pair in outdata[..samples_written].chunks_exact(2) {
                let _ = output_producer.try_push((sample_pair[0] + sample_pair[1]) / 2.0);
            }
        }
    }

    println!("Resampling thread stopped");
}
