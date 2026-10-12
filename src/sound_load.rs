use ringbuf::traits::{Observer, Producer};
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

use crate::{ActiveSound, ring_buffers::RBProducer, storage::Clip};

#[derive(Debug)]
pub struct SoundLoadingConfig {
    pub empty_buffer_retry_delay: u64,
    pub next_read_delay: u64,
    pub sounds_saved_channels: u32,
    pub generational_clear_waiting: u64,
}

pub struct SoundLoader {
    pub config: SoundLoadingConfig,
    pub flag: Arc<AtomicBool>,
    pub generation_flag: Arc<AtomicBool>,
    pub resampling_generation_flag: Arc<AtomicBool>,
    pub clips: Arc<RwLock<HashMap<Uuid, Clip>>>,
    pub active_sound: Arc<RwLock<Option<ActiveSound>>>,
    pub loaded_clip_producer: RBProducer<f32>,
}

impl SoundLoader {
    pub fn run(mut self) {
        'outer: while self.flag.load(Ordering::Relaxed) {
            let sound_id = {
                let active_sound_readable = self.active_sound.read().unwrap();

                match *active_sound_readable {
                    Some(val) => val.uuid,
                    _ => {
                        drop(active_sound_readable);
                        thread::sleep(Duration::from_millis(self.config.empty_buffer_retry_delay));
                        continue;
                    }
                }
            };

            let clips_guard = self.clips.read().unwrap();

            let (clip_path, skip_samples) = match clips_guard.get(&sound_id) {
                Some(Clip::Partial {
                    head,
                    path,
                    full_amount: _,
                }) => (path, head.len()),
                _ => {
                    drop(clips_guard);
                    thread::sleep(Duration::from_millis(self.config.empty_buffer_retry_delay));
                    continue;
                }
            };

            self.generation_flag.store(true, Ordering::Relaxed);

            let mut reader = hound::WavReader::open(&clip_path).unwrap(); // ! CHECK UNWRAP SAFETY

            let _ = reader.seek(skip_samples as u32 / self.config.sounds_saved_channels);

            loop {
                if !self.flag.load(Ordering::Relaxed) {
                    break 'outer;
                }

                if !self.generation_flag.load(Ordering::Relaxed) {
                    self.resampling_generation_flag
                        .store(false, Ordering::Relaxed);

                    while !self.resampling_generation_flag.load(Ordering::Relaxed) {
                        thread::sleep(Duration::from_micros(
                            self.config.generational_clear_waiting,
                        ));
                    }

                    continue 'outer;
                }

                let max_samples = self.loaded_clip_producer.vacant_len();

                if max_samples == 0 {
                    thread::sleep(Duration::from_secs(self.config.next_read_delay / 2));
                    continue;
                }

                let samples: Vec<f32> = match reader
                    .samples::<f32>()
                    .take(max_samples)
                    .collect::<Result<_, _>>()
                {
                    Ok(val) => val,
                    Err(err) => {
                        println!("Failed to load sound: {}", err);
                        continue;
                    }
                };

                for sample in samples {
                    let _ = self.loaded_clip_producer.try_push(sample);
                }

                thread::sleep(Duration::from_secs(self.config.next_read_delay));
            }
        }

        println!("Sound loading thread stopped");
    }
}
