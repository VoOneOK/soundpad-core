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
}

pub fn start_sound_loading(
    config: SoundLoadingConfig,
    flag: Arc<AtomicBool>,
    generation_flag: Arc<AtomicBool>,
    clips: Arc<RwLock<HashMap<Uuid, Clip>>>,
    active_sound: Arc<RwLock<Option<ActiveSound>>>,
    mut loaded_clip_producer: RBProducer<f32>,
) {
    let mut allow_override = false;

    'outer: while flag.load(Ordering::Relaxed) {
        let sound_id = {
            let active_sound_readable = active_sound.read().unwrap();

            match *active_sound_readable {
                Some(val) => val.uuid,
                _ => {
                    drop(active_sound_readable);
                    thread::sleep(Duration::from_millis(config.empty_buffer_retry_delay));
                    continue;
                }
            }
        };

        let clips_guard = clips.read().unwrap();

        let (clip_path, skip_samples) = match clips_guard.get(&sound_id) {
            Some(Clip::Partial {
                head,
                path,
                full_amount: _,
            }) => (path, head.len()),
            _ => {
                drop(clips_guard);
                thread::sleep(Duration::from_millis(config.empty_buffer_retry_delay));
                continue;
            }
        };

        generation_flag.store(true, Ordering::Relaxed);

        let mut reader = hound::WavReader::open(&clip_path).unwrap(); // ! CHECK UNWRAP SAFETY

        let _ = reader.seek(skip_samples as u32 / config.sounds_saved_channels);

        loop {
            if !flag.load(Ordering::Relaxed) {
                break 'outer;
            }

            if !generation_flag.load(Ordering::Relaxed) {
                allow_override = true;
                continue 'outer;
            }

            let max_samples = if allow_override {
                allow_override = false;
                loaded_clip_producer.vacant_len() + loaded_clip_producer.occupied_len()
            } else {
                loaded_clip_producer.vacant_len()
            };

            if max_samples == 0 {
                thread::sleep(Duration::from_secs(config.next_read_delay / 2));
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
                let _ = loaded_clip_producer.try_push(sample);
            }

            thread::sleep(Duration::from_secs(config.next_read_delay));
        }
    }

    println!("Sound loading thread stopped");
}
