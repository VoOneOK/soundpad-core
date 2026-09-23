use audioadapter_buffers::direct::InterleavedSlice;
use core::f32;
use rubato::{Async, FixedAsync, Indexing, PolynomialDegree, Resampler};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

#[derive(Debug)]
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
    keep_resampling: Arc<AtomicBool>,
    mut pop_sample: impl FnMut() -> Option<f32>,
    mut push_sample: impl FnMut(f32) -> bool,
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
        match pop_sample() {
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
                push_sample(outdata[i]);
            }
        } else if config.input_channels == 1 && config.output_channels == 2 {
            for i in 0..samples_written {
                if push_sample(outdata[i]) {
                    push_sample(outdata[i]);
                };
            }
        } else if config.input_channels == 2 && config.output_channels == 1 {
            for sample_pair in outdata[..samples_written].chunks_exact(2) {
                push_sample((sample_pair[0] + sample_pair[1]) / 2.0);
            }
        }
    }

    println!("Resampling thread stopped");
}
