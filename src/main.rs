use core::f32;
use cpal::{
    Host,
    traits::{DeviceTrait, StreamTrait},
};
use ringbuf::traits::{Consumer, Observer, Producer};
use std::{fmt::Write, sync::Mutex};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};
use uuid::Uuid;

use crate::resample::PoppedSample;
use crate::resample::ResampleConfig;

mod devices;
mod resample;
mod ring_buffers;
mod sounds;
mod storage;
mod ui;

#[derive(Debug)]
struct SoundpadSettings {
    input_buffer_divider: usize,
    output_buffer_divider: usize,
    clip_buffer_multiplier: usize,
    resampling_chunk_size: usize,
    resampling_start_delay_ms: u64,
    resampling_buffer_fill_retry_delay_ms: u64,
    max_preload_size_mb: u32,
    storage_paths: storage::StoragePaths,
}

#[derive(Debug, Clone, Copy)]
struct ActiveSound {
    uuid: uuid::Uuid,
    position: usize,
}

fn main() {
    const QUALIFIER: &str = "net";
    const AUTHOR: &str = "vooneok";
    const APP: &str = "open-soundpad-core";

    let storage_paths = match storage::storage_paths(QUALIFIER, AUTHOR, APP) {
        Ok(val) => val,
        Err(error) => {
            println!("{}", error);
            return;
        }
    };

    let mut soundpad_settings = SoundpadSettings {
        input_buffer_divider: 5,
        output_buffer_divider: 5,
        clip_buffer_multiplier: 2,
        resampling_chunk_size: 2024,
        resampling_start_delay_ms: 100,
        resampling_buffer_fill_retry_delay_ms: 1,
        max_preload_size_mb: 5,
        storage_paths,
    };

    let host = cpal::default_host();

    loop {
        if run_soundpad(&host, &mut soundpad_settings) {
            break;
        }
    }
}

fn run_soundpad(host: &Host, settings: &mut SoundpadSettings) -> bool {
    const CLIPS_SAMPLES: f64 = 48000.0;
    const CLIPS_CHANNELS: usize = 2;

    let mut sounds_config = match storage::read_sounds_config(&settings.storage_paths.config.sounds)
    {
        Ok(val) => val,
        Err(error) => {
            println!("{}", error);
            return true;
        }
    };

    let clips = storage::verify_and_load_sounds(
        &mut sounds_config.sounds,
        &settings.storage_paths.data.sounds_dir,
        (settings.max_preload_size_mb * 1024 * 1024 / 4) as usize,
    );

    let active_sound: Arc<Mutex<Option<ActiveSound>>> = Arc::new(Mutex::new(None));
    let active_sound_clone = Arc::clone(&active_sound);

    let (input_device, input_config) = devices::get_input_device(&host);
    let (output_device, output_config) =
        devices::get_output_device(&host, input_config.sample_rate);

    let (mut input_producer, mut input_consumer) = ring_buffers::create_ring_buffer::<f32>(
        input_config.sample_rate as usize * input_config.channels as usize
            / settings.input_buffer_divider,
    );

    let (mut output_producer, mut output_consumer) = ring_buffers::create_ring_buffer::<f32>(
        output_config.sample_rate as usize * output_config.channels as usize
            / settings.output_buffer_divider,
    );

    let clip_buffer_len = output_config.sample_rate as usize
        * output_config.channels as usize
        * settings.clip_buffer_multiplier;

    let (mut clip_producer, mut clip_consumer) =
        ring_buffers::create_ring_buffer::<f32>(clip_buffer_len);

    let input_stream = input_device
        .build_input_stream(
            input_config,
            move |data: &[f32], &_: &cpal::InputCallbackInfo| {
                for sample in data {
                    let _ = input_producer.try_push(*sample);
                }
            },
            move |err| print!("Input stream error {}", err),
            None,
        )
        .expect("Failed to build input stream");

    let output_stream = output_device
        .build_output_stream(
            output_config,
            move |data: &mut [f32], _: &cpal::OutputCallbackInfo| {
                for sample in data {
                    let mic_sample = output_consumer.try_pop().unwrap_or(0.0);
                    let clip_sample = clip_consumer.try_pop().unwrap_or(0.0);

                    *sample = (mic_sample + clip_sample).clamp(-1.0, 1.0);
                }
            },
            move |err| print!("Output stream error {}", err),
            None,
        )
        .expect("Failed to build output stream");

    input_stream.play().expect("Input stream failed to start");
    output_stream.play().expect("Output stream failed to start");

    let keep_resampling = Arc::new(AtomicBool::new(true));
    let mic_flag = keep_resampling.clone();
    let clips_flag = keep_resampling.clone();

    let mic_resampling_ratio = output_config.sample_rate as f64 / input_config.sample_rate as f64;

    let mic_resample_config = ResampleConfig {
        input_channels: input_config.channels as usize,
        output_channels: output_config.channels as usize,
        chunk_size: settings.resampling_chunk_size,
        ratio: mic_resampling_ratio,
        start_delay: settings.resampling_start_delay_ms,
        empty_buffer_retry_delay: settings.resampling_buffer_fill_retry_delay_ms,
        unknown_buffer_fullness: 0.0,
    };

    let mic_resample_thread = thread::spawn(move || {
        resample::start_resampling_loop(
            mic_resample_config,
            mic_flag,
            || match input_consumer.try_pop() {
                Some(val) => PoppedSample::Ready(val),
                _ => PoppedSample::Waiting,
            },
            |sample| {
                (output_producer.try_push(sample).is_ok(), 0.0) // 0.0 for no delay
            },
        );
    });

    let clips_resample_config = ResampleConfig {
        input_channels: 2,
        output_channels: CLIPS_CHANNELS,
        chunk_size: settings.resampling_chunk_size,
        ratio: output_config.sample_rate as f64 / CLIPS_SAMPLES,
        start_delay: settings.resampling_start_delay_ms,
        empty_buffer_retry_delay: settings.resampling_buffer_fill_retry_delay_ms,
        unknown_buffer_fullness: 0.0,
    };

    let clips_resample_thread = thread::spawn(move || {
        resample::start_resampling_loop(
            clips_resample_config,
            clips_flag,
            || {
                let mut active_sound_guard =
                    active_sound_clone.lock().unwrap_or_else(|e| e.into_inner());

                if active_sound_guard.is_none() {
                    return PoppedSample::Waiting;
                }

                let clip = match clips.get(&active_sound_guard.as_ref().unwrap().uuid) {
                    Some(val) => val,
                    _ => {
                        return PoppedSample::Waiting;
                    }
                };

                match clip {
                    storage::Clip::Preloaded(samples) => {
                        let position = active_sound_guard.as_ref().unwrap().position;

                        if samples.len() == position {
                            let val = PoppedSample::Ended;
                            *active_sound_guard = None;
                            val
                        } else {
                            let val = PoppedSample::Ready(samples[position]);
                            active_sound_guard.as_mut().unwrap().position += 1;
                            val
                        }
                    }
                    storage::Clip::Partial {
                        head: _,
                        path: _,
                        samples_read: _,
                    } => {
                        // TODO
                        PoppedSample::Ready(3.0)
                    }
                    storage::Clip::NotLoaded { error: _, path: _ } => PoppedSample::Waiting,
                }
            },
            |sample| {
                (
                    clip_producer.try_push(sample).is_ok(),
                    clip_producer.occupied_len() as f64 / clip_buffer_len as f64,
                )
            },
        );
    });

    let input_name = input_device.description().unwrap().name().to_string();
    let output_name = output_device.description().unwrap().name().to_string();

    let ui_context = ui::UIContext {
        input: ui::UIDevice {
            name: input_name,
            rate: input_config.sample_rate,
            channels: input_config.channels,
        },
        output: ui::UIDevice {
            name: output_name,
            rate: output_config.sample_rate,
            channels: output_config.channels,
        },
        ratio: mic_resampling_ratio,
    };

    let mut last_output = String::new();

    let is_exiting: bool = loop {
        let command = match ui::run_ui(&ui_context, &last_output) {
            Ok(val) => val,
            Err(err) => {
                last_output = err;
                continue;
            }
        };

        let parts: Vec<&str> = command.split_whitespace().collect();

        let Some(first) = parts.first() else {
            last_output = String::from("No command entered");
            continue;
        };

        match *first {
            "exit" => {
                break true;
            }
            "restart" => {
                break false;
            }
            "upload" => {
                if parts.len() < 3 {
                    last_output = "Provide name and path to file".into();
                    continue;
                }

                let sound_id = Uuid::new_v4();

                if let Err(err) = sounds::upload_sound(
                    &sound_id.to_string(),
                    parts[2],
                    &settings.storage_paths.data.sounds_dir,
                ) {
                    last_output = err;
                    continue;
                }

                sounds_config.sounds.push(storage::Sound {
                    uuid: sound_id,
                    name: String::from(parts[1]),
                });

                if let Err(err) = storage::write_sounds_config(
                    &settings.storage_paths.config.sounds,
                    &sounds_config,
                ) {
                    last_output = err;
                    continue;
                }

                last_output = format!("Uploaded {} ({})", parts[1], sound_id);
            }
            "list" => {
                if sounds_config.sounds.is_empty() {
                    last_output = "No sounds uploaded".into();
                    continue;
                }

                last_output.clear();
                for sound in &sounds_config.sounds {
                    let _ = writeln!(last_output, "{} ({})", sound.name, sound.uuid);
                }
                last_output.pop();
                continue;
            }
            "play" => {
                if parts.len() < 2 {
                    last_output = "Provide sound's uuid".into();
                    continue;
                }

                match Uuid::parse_str(parts[1]) {
                    Ok(sound_id) => {
                        let mut active_sound_guard =
                            active_sound.lock().unwrap_or_else(|e| e.into_inner());

                        *active_sound_guard = Some(ActiveSound {
                            uuid: sound_id,
                            position: 0,
                        });

                        last_output = "Playing...".into();
                    }
                    Err(_err) => {
                        last_output = "Provide sound's uuid".into();
                    }
                };
            }
            _ => {
                last_output = format!("No command \"{}\"", parts[0]);
            }
        }
    };

    keep_resampling.store(false, Ordering::Relaxed);
    let _ = mic_resample_thread.join();
    let _ = clips_resample_thread.join();

    is_exiting
}
