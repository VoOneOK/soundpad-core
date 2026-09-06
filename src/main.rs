use core::f32;
use cpal::{
    Host,
    traits::{DeviceTrait, StreamTrait},
};
use ringbuf::traits::{Consumer, Producer};
use std::{
    io::{self, Write as _},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

mod devices;
mod resample;
mod ring_buffers;

struct SoundpadSettings {
    input_buffer_divider: usize,
    output_buffer_divider: usize,
    resampling_chunk_size: usize,
    resampling_start_delay_ms: u64,
    resampling_buffer_fill_retry_delay_ms: u64,
}

struct IO {
    name: String,
    rate: u32,
    channels: u16,
}

struct UIData {
    input: IO,
    output: IO,
    ratio: f64,
}

fn main() {
    let mut soundpad_settings = SoundpadSettings {
        input_buffer_divider: 5,
        output_buffer_divider: 5,
        resampling_chunk_size: 2024,
        resampling_start_delay_ms: 100,
        resampling_buffer_fill_retry_delay_ms: 1,
    };

    let host = cpal::default_host();

    loop {
        if run_soundpad(&host, &mut soundpad_settings) {
            break;
        }
    }
}

fn run_soundpad(host: &Host, settings: &mut SoundpadSettings) -> bool {
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

    let ui_data = UIData {
        input: IO {
            name: input_name,
            rate: input_config.sample_rate,
            channels: input_config.channels,
        },
        output: IO {
            name: output_name,
            rate: output_config.sample_rate,
            channels: output_config.channels,
        },
        ratio: resampling_ratio,
    };

    let is_exiting = run_ui(ui_data);

    keep_resampling.store(false, Ordering::Relaxed);
    let _ = resample_thread.join();

    is_exiting
}

fn run_ui(data: UIData) -> bool {
    let mut last_output = String::new();

    loop {
        println!("{}[2J", 27 as char);

        println!("Commands                                         | State");
        println!(
            "  exit - exits soundpad                          |   Input: {} ({} * {})",
            data.input.name, data.input.channels, data.input.rate,
        );
        println!(
            "  restart - restarts app                         |   Output: {} ({} * {})",
            data.output.name, data.output.channels, data.output.rate,
        );
        println!(
            "  upload x path_to_file - upload sound to slot x |   Ratio: {:.6}",
            data.ratio
        );
        println!("  play x - play sound x                          |   ");

        println!("{}", last_output);
        print!("> ");
        io::stdout().flush().unwrap();

        let mut command = String::new();

        match io::stdin().read_line(&mut command) {
            Ok(_) => {
                let parts: Vec<&str> = command.split_whitespace().collect();

                match parts[0] {
                    "exit" => {
                        return true;
                    }
                    "restart" => {
                        return false;
                    }
                    _ => {
                        last_output = format!("No command \"{}\"", parts[0]);
                        continue;
                    }
                }
            }
            Err(e) => {
                eprintln!("Input error: {}. Try again", e);
                continue;
            }
        }
    }
}
