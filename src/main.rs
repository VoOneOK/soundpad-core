use audioadapter_buffers::direct::InterleavedSlice;
use core::f32;
use cpal::traits::{DeviceTrait, StreamTrait};
use ringbuf::traits::{Consumer, Producer};
use rubato::{Async, FixedAsync, Indexing, PolynomialDegree, Resampler};
use std::{thread, time::Duration};

mod devices;
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

    let (mut input_producer, mut input_consumer) = ring_buffers::create_ring_buffer::<f32>({
        input_config.sample_rate as usize * input_config.channels as usize / INPUT_BUFFER_DIVIDER
    });

    let (mut output_producer, mut output_consumer) = ring_buffers::create_ring_buffer::<f32>({
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
                    let a = output_consumer.try_pop().unwrap_or(0.0);
                    // println!("{}", a);
                    *sample = a;
                    // println!("{}", sample);
                }
            },
            move |err| print!("Output stream error {}", err),
            None,
        )
        .expect("Failed to build output stream");

    input_stream.play().expect("Input stream failed to start");
    output_stream.play().expect("Output stream failed to start");

    let channels = input_config.channels as usize;
    let chunk_size = RESAMPLING_CHUNK_SIZE;

    let mut resampler = Async::<f32>::new_poly(
        output_config.sample_rate as f64 / input_config.sample_rate as f64,
        1.1,
        PolynomialDegree::Cubic,
        chunk_size,
        channels,
        FixedAsync::Input,
    )
    .unwrap();

    let mut frames_to_read = resampler.input_frames_next();
    let mut samples_to_read = frames_to_read * channels;
    let mut indata: Vec<f32> = Vec::with_capacity(samples_to_read);
    let mut outdata = vec![0.0; channels * resampler.output_frames_max()];
    let outdata_capacity = outdata.len() / channels;

    let indexing = Indexing::new();

    thread::sleep(Duration::from_millis(RESAMPLING_START_DELAY_MS));

    loop {
        match input_consumer.try_pop() {
            Some(sample) => {
                indata.push(sample);
            }
            None => {
                thread::sleep(Duration::from_millis(RESAMPLING_BUFFER_FILL_RETRY_DELAY_MS));
                continue;
            }
        }

        if indata.len() < samples_to_read {
            continue;
        }

        let input_adapter = InterleavedSlice::new(&indata, channels, frames_to_read).unwrap();
        let mut output_adapter =
            InterleavedSlice::new_mut(&mut outdata, channels, outdata_capacity).unwrap();

        let (_, frames_written) = resampler
            .process_into_buffer(&input_adapter, &mut output_adapter, Some(&indexing))
            .unwrap();

        indata.clear();

        frames_to_read = resampler.input_frames_next();
        samples_to_read = frames_to_read * channels;

        let samples_written = frames_written * channels;

        if input_config.channels == output_config.channels {
            for i in 0..samples_written {
                let _ = output_producer.try_push(outdata[i]);
            }
        } else if input_config.channels == 1 && output_config.channels == 2 {
            for i in 0..samples_written {
                let _ = output_producer.try_push(outdata[i]);
                let _ = output_producer.try_push(outdata[i]);
            }
        } else if input_config.channels == 2 && output_config.channels == 1 {
            for sample_pair in outdata[..samples_written].chunks_exact(2) {
                let _ = output_producer.try_push((sample_pair[0] + sample_pair[1]) / 2.0);
            }
        }
    }
}
