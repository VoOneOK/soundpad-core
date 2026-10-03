use core::f32;
use cpal::{
    Host,
    traits::{DeviceTrait, StreamTrait},
};
use ringbuf::traits::{Consumer, Producer};
use std::{
    collections::HashMap,
    fmt::Write,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};
use uuid::Uuid;

use crate::{
    resample::ResampleConfig,
    storage::{Clip, SoundsConfig},
};

mod devices;
mod resample;
mod ring_buffers;
mod sounds;
mod storage;
mod ui;

#[derive(Debug)]
struct SoundpadContext {
    input_buffer_divider: usize,
    output_buffer_divider: usize,
    clip_buffer_multiplier: usize,
    resampling_chunk_size: usize,
    resampling_start_delay_ms: u64,
    resampling_buffer_fill_retry_delay_ms: u64,
    storage_paths: storage::StoragePaths,
    sounds_config: SoundsConfig,
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

    // later will be read out of saved config anyway
    let max_preload_size_mb: u32 = 5;

    let storage_paths = match storage::storage_paths(QUALIFIER, AUTHOR, APP) {
        Ok(val) => val,
        Err(error) => {
            println!("{}", error);
            return;
        }
    };

    let mut sounds_config = match storage::read_sounds_config(&storage_paths.config.sounds) {
        Ok(val) => val,
        Err(error) => {
            println!("{}", error);
            return;
        }
    };

    println!("Preloading sounds...");

    let clips = Arc::new(RwLock::new(storage::verify_and_load_sounds(
        &mut sounds_config.sounds,
        &storage_paths.data.sounds_dir,
        (max_preload_size_mb * 1024 * 1024 / 4) as usize,
    )));

    let host = cpal::default_host();

    let mut soundpad_ctx = SoundpadContext {
        input_buffer_divider: 5,
        output_buffer_divider: 5,
        clip_buffer_multiplier: 2,
        resampling_chunk_size: 2024,
        resampling_start_delay_ms: 100,
        resampling_buffer_fill_retry_delay_ms: 1,
        storage_paths,
        sounds_config,
    };

    loop {
        if run_soundpad(&host, &mut soundpad_ctx, &clips) {
            break;
        }
    }
}

fn run_soundpad(
    host: &Host,
    context: &mut SoundpadContext,
    clips: &Arc<RwLock<HashMap<Uuid, Clip>>>,
) -> bool {
    const CLIPS_SAMPLES: f64 = 48000.0;
    const CLIPS_CHANNELS: usize = 2;

    let active_sound: Arc<RwLock<Option<ActiveSound>>> = Arc::new(RwLock::new(None));

    let (input_device, input_config) = devices::get_input_device(&host);
    let (output_device, output_config) =
        devices::get_output_device(&host, input_config.sample_rate);

    let (mut input_producer, input_consumer) = ring_buffers::create_ring_buffer::<f32>(
        input_config.sample_rate as usize * input_config.channels as usize
            / context.input_buffer_divider,
    );

    let (output_producer, mut output_consumer) = ring_buffers::create_ring_buffer::<f32>(
        output_config.sample_rate as usize * output_config.channels as usize
            / context.output_buffer_divider,
    );

    let clip_buffer_len = output_config.sample_rate as usize
        * output_config.channels as usize
        * context.clip_buffer_multiplier;

    let (clip_producer, mut clip_consumer) =
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
    let mic_resampling_ratio = output_config.sample_rate as f64 / input_config.sample_rate as f64;
    let mic_resample_config = ResampleConfig {
        input_channels: input_config.channels as usize,
        output_channels: output_config.channels as usize,
        chunk_size: context.resampling_chunk_size,
        ratio: mic_resampling_ratio,
        start_delay: context.resampling_start_delay_ms,
        empty_buffer_retry_delay: context.resampling_buffer_fill_retry_delay_ms,
        unknown_buffer_fullness: 0.0,
    };

    let mic_resample_thread = thread::spawn(move || {
        resample::start_mic_resampling(
            mic_resample_config,
            mic_flag,
            input_consumer,
            output_producer,
        );
    });

    let clips_flag = keep_resampling.clone();
    let clips_clone = Arc::clone(clips);
    let active_sound_clone = Arc::clone(&active_sound);
    let clips_resample_config = ResampleConfig {
        input_channels: 2,
        output_channels: CLIPS_CHANNELS,
        chunk_size: context.resampling_chunk_size,
        ratio: output_config.sample_rate as f64 / CLIPS_SAMPLES,
        start_delay: context.resampling_start_delay_ms,
        empty_buffer_retry_delay: context.resampling_buffer_fill_retry_delay_ms,
        unknown_buffer_fullness: 0.0,
    };

    let clips_resample_thread = thread::spawn(move || {
        resample::start_clips_resampling(
            clips_resample_config,
            clips_flag,
            clips_clone,
            active_sound_clone,
            clip_buffer_len,
            clip_producer,
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
                    &context.storage_paths.data.sounds_dir,
                ) {
                    last_output = err;
                    continue;
                }

                context.sounds_config.sounds.push(storage::Sound {
                    uuid: sound_id,
                    name: String::from(parts[1]),
                });

                if let Err(err) = storage::write_sounds_config(
                    &context.storage_paths.config.sounds,
                    &context.sounds_config,
                ) {
                    last_output = err;
                    continue;
                }

                last_output = format!("Uploaded {} ({})", parts[1], sound_id);
            }
            "list" => {
                if context.sounds_config.sounds.is_empty() {
                    last_output = "No sounds uploaded".into();
                    continue;
                }

                last_output.clear();
                for sound in &context.sounds_config.sounds {
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
                        let mut active_sound_writable = active_sound.write().unwrap();

                        *active_sound_writable = Some(ActiveSound {
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
