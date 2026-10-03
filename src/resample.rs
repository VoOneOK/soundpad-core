use audioadapter_buffers::direct::InterleavedSlice;
use core::f32;
use ringbuf::traits::{Consumer, Observer, Producer};
use rubato::{Async, FixedAsync, Indexing, PolynomialDegree, Resampler};
use std::{
    collections::HashMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use uuid::Uuid;

use crate::{
    ActiveSound,
    ring_buffers::{RBConsumer, RBProducer},
    storage::Clip,
};

#[derive(Debug)]
pub struct ResampleConfig {
    pub input_channels: usize,
    pub output_channels: usize,
    pub chunk_size: usize,
    pub ratio: f64,
    pub start_delay: u64,
    pub empty_buffer_retry_delay: u64,
    pub unknown_buffer_fullness: f64,
}

pub fn start_mic_resampling(
    config: ResampleConfig,
    flag: Arc<AtomicBool>,
    mut input_consumer: RBConsumer<f32>,
    mut output_producer: RBProducer<f32>,
) {
    let empty_buffer_retry_delay = config.empty_buffer_retry_delay;

    let fill_indata = |indata: &mut Vec<f32>, amount| -> bool {
        if !flag.load(Ordering::Relaxed) {
            return true;
        }

        while indata.len() < amount {
            match input_consumer.try_pop() {
                Some(sample) => indata.push(sample),
                None => {
                    thread::sleep(Duration::from_millis(empty_buffer_retry_delay));
                }
            }
        }

        false
    };

    let push_sample =
        |sample: f32| -> (bool, f64) { (output_producer.try_push(sample).is_ok(), 0.0) };

    start_resampling_loop(config, fill_indata, push_sample);

    println!("Mic resampling thread stopped");
}

pub fn start_clips_resampling(
    config: ResampleConfig,
    flag: Arc<AtomicBool>,
    clips: Arc<RwLock<HashMap<Uuid, Clip>>>,
    active_sound: Arc<RwLock<Option<ActiveSound>>>,
    clip_buffer_len: usize,
    mut output_producer: RBProducer<f32>,
) {
    let empty_buffer_retry_delay = config.empty_buffer_retry_delay;

    let fill_indata = |indata: &mut Vec<f32>, amount| -> bool {
        while indata.len() < amount {
            if !flag.load(Ordering::Relaxed) {
                return true;
            }

            let (uuid, start_position) = {
                let active_sound_readable = active_sound.read().unwrap();

                let active_sound_readable = match active_sound_readable.as_ref() {
                    Some(val) => val,
                    _ => {
                        thread::sleep(Duration::from_millis(empty_buffer_retry_delay));
                        continue;
                    }
                };

                (active_sound_readable.uuid, active_sound_readable.position)
            };

            let clips_readable = clips.read().unwrap();

            let clip = match clips_readable.get(&uuid) {
                Some(val) => val,
                _ => {
                    thread::sleep(Duration::from_millis(empty_buffer_retry_delay));
                    continue;
                }
            };

            match clip {
                Clip::Preloaded(samples) => {
                    let samples_to_push = amount.min(samples.len() - start_position);

                    for i in 0..samples_to_push {
                        indata.push(samples[start_position + i]);
                    }

                    let mut active_sound_writable = active_sound.write().unwrap();

                    if samples.len() == start_position + samples_to_push {
                        *active_sound_writable = None;
                    } else {
                        active_sound_writable.as_mut().unwrap().position += samples_to_push;
                    }

                    if indata.len() < amount {
                        indata.resize(amount, 0.0);
                    }
                }
                Clip::Partial {
                    head: _,
                    path: _,
                    samples_read: _,
                } => {
                    // TODO
                    indata.resize(amount, 0.0);
                }
                Clip::NotLoaded { error: _, path: _ } => indata.resize(amount, 0.0),
            };
        }

        false
    };

    let push_sample = |sample: f32| -> (bool, f64) {
        (
            output_producer.try_push(sample).is_ok(),
            output_producer.occupied_len() as f64 / clip_buffer_len as f64,
        )
    };

    start_resampling_loop(config, fill_indata, push_sample);

    println!("Clips resampling thread stopped");
}

pub fn start_resampling_loop(
    config: ResampleConfig,
    mut fill_indata: impl FnMut(&mut Vec<f32>, usize) -> bool,
    mut push_sample: impl FnMut(f32) -> (bool, f64),
) -> () {
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
    loop {
        if fill_indata(&mut indata, samples_to_read) {
            return;
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

        let buffer_fullness = if config.input_channels == config.output_channels {
            for i in 0..samples_written.saturating_sub(1) {
                push_sample(outdata[i]);
            }
            push_sample(outdata[samples_written - 1]).1
        } else if config.input_channels == 1 && config.output_channels == 2 {
            let mut ratio = config.unknown_buffer_fullness;

            for i in 0..samples_written.saturating_sub(1) {
                if push_sample(outdata[i]).0 {
                    ratio = push_sample(outdata[i]).1;
                }
            }

            ratio
        } else if config.input_channels == 2 && config.output_channels == 1 {
            let mut ratio = config.unknown_buffer_fullness;

            for sample_pair in outdata[..samples_written].chunks_exact(2) {
                ratio = push_sample((sample_pair[0] + sample_pair[1]) / 2.0).1;
            }

            ratio
        } else {
            config.unknown_buffer_fullness
        };

        if buffer_fullness > 0.5 {
            thread::sleep(Duration::from_micros(
                (buffer_fullness * 1000.0 * 1000.0) as u64,
            ));
        }
    }
}
