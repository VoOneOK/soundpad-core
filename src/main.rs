use core::f32;
use cpal::{
    Host,
    traits::{DeviceTrait, StreamTrait},
};
use ringbuf::traits::{Consumer, Producer};
use std::fmt::Write;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};
use uuid::Uuid;

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
    resampling_chunk_size: usize,
    resampling_start_delay_ms: u64,
    resampling_buffer_fill_retry_delay_ms: u64,
    max_preload_size_mb: u32,
    storage_paths: storage::StoragePaths,
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

    let (input_device, input_config) = devices::get_input_device(&host);
    let (output_device, output_config) =
        devices::get_output_device(&host, input_config.sample_rate);

    let (mut input_producer, input_consumer) = ring_buffers::create_ring_buffer::<f32>({
        input_config.sample_rate as usize * input_config.channels as usize
            / settings.input_buffer_divider
    });

    let (output_producer, mut output_consumer) = ring_buffers::create_ring_buffer::<f32>({
        output_config.sample_rate as usize * output_config.channels as usize
            / settings.output_buffer_divider
    });

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
                    *sample = output_consumer.try_pop().unwrap_or(0.0);
                }
            },
            move |err| print!("Output stream error {}", err),
            None,
        )
        .expect("Failed to build output stream");

    input_stream.play().expect("Input stream failed to start");
    output_stream.play().expect("Output stream failed to start");

    let resampling_ratio = output_config.sample_rate as f64 / input_config.sample_rate as f64;

    let resample_config = resample::ResampleConfig {
        input_channels: input_config.channels as usize,
        output_channels: output_config.channels as usize,
        chunk_size: settings.resampling_chunk_size,
        ratio: resampling_ratio,
        start_delay: settings.resampling_start_delay_ms,
        empty_buffer_retry_delay: settings.resampling_buffer_fill_retry_delay_ms,
    };

    let keep_resampling = Arc::new(AtomicBool::new(true));
    let keep_resampling_clone = keep_resampling.clone();

    let resample_thread = thread::spawn(move || {
        resample::start_resampling_loop(
            resample_config,
            input_consumer,
            output_producer,
            keep_resampling_clone,
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
        ratio: resampling_ratio,
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
            _ => {
                last_output = format!("No command \"{}\"", parts[0]);
            }
        }
    };

    keep_resampling.store(false, Ordering::Relaxed);
    let _ = resample_thread.join();

    is_exiting
}
