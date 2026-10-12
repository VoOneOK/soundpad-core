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
    pub unknown_buffer_fullness: f64,
    pub start_delay: Duration,
    pub empty_buffer_retry_delay: Duration,
}

pub struct MicResampler {
    pub config: ResampleConfig,
    pub flag: Arc<AtomicBool>,
    pub input_consumer: RBConsumer<f32>,
    pub output_producer: RBProducer<f32>,
}

impl MicResampler {
    pub fn run(mut self) {
        let empty_buffer_retry_delay = self.config.empty_buffer_retry_delay;

        let fill_indata = |indata: &mut Vec<f32>, amount| -> bool {
            if !self.flag.load(Ordering::Relaxed) {
                return true;
            }

            while indata.len() < amount {
                match self.input_consumer.try_pop() {
                    Some(sample) => indata.push(sample),
                    None => {
                        thread::sleep(empty_buffer_retry_delay);
                    }
                }
            }

            false
        };

        let push_sample =
            |sample: f32| -> (bool, f64) { (self.output_producer.try_push(sample).is_ok(), 0.0) };

        start_resampling_loop(self.config, fill_indata, push_sample);

        println!("Mic resampling thread stopped");
    }
}

pub struct ClipsResampler {
    pub config: ResampleConfig,
    pub flag: Arc<AtomicBool>,
    pub generation_flag: Arc<AtomicBool>,
    pub clips: Arc<RwLock<HashMap<Uuid, Clip>>>,
    pub active_sound: Arc<RwLock<Option<ActiveSound>>>,
    pub clip_buffer_len: usize,
    pub loaded_clip_consumer: RBConsumer<f32>,
    pub output_producer: RBProducer<f32>,
}

impl ClipsResampler {
    pub fn run(mut self) {
        let empty_buffer_retry_delay = self.config.empty_buffer_retry_delay;

        let fill_indata = |indata: &mut Vec<f32>, amount: usize| -> bool {
            'outer: while indata.len() < amount {
                if !self.flag.load(Ordering::Relaxed) {
                    return true;
                }

                if !self.generation_flag.load(Ordering::Relaxed) {
                    indata.clear();
                    self.loaded_clip_consumer.clear();
                    self.generation_flag.store(true, Ordering::Relaxed);
                }

                let (uuid, start_position) = {
                    let active_sound_readable = self.active_sound.read().unwrap();

                    let active_sound_readable = match active_sound_readable.as_ref() {
                        Some(val) => val,
                        _ => {
                            drop(active_sound_readable);
                            thread::sleep(empty_buffer_retry_delay);
                            continue;
                        }
                    };

                    (active_sound_readable.uuid, active_sound_readable.position)
                };

                let clips_readable = self.clips.read().unwrap();

                let clip = match clips_readable.get(&uuid) {
                    Some(val) => val,
                    _ => {
                        drop(clips_readable);
                        let mut active_sound_writable = self.active_sound.write().unwrap();
                        *active_sound_writable = None;
                        thread::sleep(empty_buffer_retry_delay);
                        continue;
                    }
                };

                let (pushed_samples, total_samples) = match clip {
                    Clip::Preloaded(samples) => {
                        let samples_to_push = amount.min(samples.len() - start_position);
                        indata.extend_from_slice(
                            &samples[start_position..(start_position + samples_to_push)],
                        );

                        (samples_to_push, samples.len())
                    }
                    Clip::Partial {
                        head,
                        path: _,
                        full_amount,
                    } => {
                        let full_amount = *full_amount;
                        let preloaded_amount = head.len();

                        let mut pushed_samples = if preloaded_amount > start_position {
                            let samples_to_push = amount.min(preloaded_amount - start_position);
                            indata.extend_from_slice(
                                &head[start_position..(start_position + samples_to_push)],
                            );

                            samples_to_push
                        } else {
                            0
                        };

                        drop(clips_readable);

                        if indata.len() < amount && indata.len() + start_position < full_amount {
                            while indata.len() < amount {
                                match self.loaded_clip_consumer.try_pop() {
                                    Some(sample) => {
                                        pushed_samples += 1;
                                        indata.push(sample);
                                    }
                                    _ => {
                                        if !self.flag.load(Ordering::Relaxed) {
                                            return true;
                                        }
                                        if !self.generation_flag.load(Ordering::Relaxed) {
                                            continue 'outer;
                                        }
                                        thread::sleep(empty_buffer_retry_delay);
                                    }
                                };
                            }
                        }

                        (pushed_samples, full_amount)
                    }
                    Clip::NotLoaded { error: _, path: _ } => {
                        let mut active_sound_writable = self.active_sound.write().unwrap();
                        *active_sound_writable = None;
                        continue;
                    }
                };

                if indata.len() < amount && !indata.is_empty() {
                    indata.resize(amount, 0.0);
                }

                if pushed_samples == 0 {
                    continue;
                }

                let mut active_sound_writable = self.active_sound.write().unwrap();

                if total_samples == start_position + pushed_samples {
                    *active_sound_writable = None;
                } else {
                    active_sound_writable.as_mut().unwrap().position += pushed_samples;
                }
            }

            false
        };

        let push_sample = |sample: f32| -> (bool, f64) {
            (
                self.output_producer.try_push(sample).is_ok(),
                self.output_producer.occupied_len() as f64 / self.clip_buffer_len as f64,
            )
        };

        start_resampling_loop(self.config, fill_indata, push_sample);

        println!("Clips resampling thread stopped");
    }
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

    thread::sleep(config.start_delay);
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
