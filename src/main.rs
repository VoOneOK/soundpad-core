use core::f32;
use cpal::traits::{DeviceTrait, StreamTrait};
use ringbuf::traits::{Consumer, Producer};

mod devices;
mod resample;
mod ring_buffers;

fn main() {
    const INPUT_BUFFER_DIVIDER: usize = 5;
    const OUTPUT_BUFFER_DIVIDER: usize = 5;
    const RESAMPLING_CHUNK_SIZE: usize = 2024;
    const RESAMPLING_START_DELAY_MS: u64 = 100;
    const RESAMPLING_BUFFER_FILL_RETRY_DELAY_MS: u64 = 1;

    let host = cpal::default_host();

    let (input_device, input_config) = devices::get_input_device(&host);
    let (output_device, output_config) = devices::get_output_device(&host);

    println!("Input:");
    println!("  Channels: {}", input_config.channels);
    println!("  Sample rate: {}", input_config.sample_rate);

    println!("Output:");
    println!("  Channels: {}", output_config.channels);
    println!("  Sample rate: {}", output_config.sample_rate);

    let (mut input_producer, input_consumer) = ring_buffers::create_ring_buffer::<f32>({
        input_config.sample_rate as usize * input_config.channels as usize / INPUT_BUFFER_DIVIDER
    });

    let (output_producer, mut output_consumer) = ring_buffers::create_ring_buffer::<f32>({
        output_config.sample_rate as usize * output_config.channels as usize / OUTPUT_BUFFER_DIVIDER
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

    let resample_config = resample::ResampleConfig {
        input_channels: input_config.channels as usize,
        output_channels: output_config.channels as usize,
        chunk_size: RESAMPLING_CHUNK_SIZE,
        ratio: output_config.sample_rate as f64 / input_config.sample_rate as f64,
        start_delay: RESAMPLING_START_DELAY_MS,
        empty_buffer_retry_delay: RESAMPLING_BUFFER_FILL_RETRY_DELAY_MS,
    };

    resample::start_resampling_loop(resample_config, input_consumer, output_producer);
}
