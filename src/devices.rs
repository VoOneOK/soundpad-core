use cpal::{
    Device, DeviceId, Host, StreamConfig,
    traits::{DeviceTrait, HostTrait},
};
use std::io::{self, Write};

pub fn get_input_device(host: &Host) -> (Device, StreamConfig) {
    let raw_devices = host.input_devices().expect("Failed to get input devices");
    let mut devices: Vec<DeviceId> = Vec::new();

    println!("Available microphones:");

    for (i, candidate) in raw_devices.enumerate() {
        let candidate_id = candidate.id().expect("Failed to get device's id");
        let candidate_desc = candidate
            .description()
            .expect("Failed to get device's description");
        let candidate_name = candidate_desc.name();
        devices.push(candidate_id);
        println!("{}. {}", i + 1, candidate_name);
    }

    println!("\nSelect one (1-{})", devices.len());

    let device = select_input_device(devices.len());

    let device = &devices[device];
    let device = host
        .device_by_id(device)
        .expect("Device probably got disconnected");

    let device_desc = device.description().unwrap();
    println!("Selected: {}", device_desc.name());

    let mut supported_configs_range = device
        .supported_input_configs()
        .expect("Failed to get input configs");

    let config: cpal::StreamConfig = supported_configs_range
        .next()
        .expect("no supported input config?!")
        .with_max_sample_rate()
        .into();

    (device, config)
}

fn select_input_device(max_option: usize) -> usize {
    loop {
        print!("> ");
        io::stdout().flush().unwrap();

        let mut selected_device = String::new();

        match io::stdin().read_line(&mut selected_device) {
            Ok(_) => {
                let selected_device = match selected_device.trim().parse::<usize>() {
                    Ok(num) if num >= 1 && num <= max_option => num - 1,
                    _ => {
                        eprintln!("Input a valid number");
                        continue;
                    }
                };
                return selected_device;
            }
            Err(e) => {
                eprintln!("Input error: {}. Try again", e);
                continue;
            }
        }
    }
}

pub fn get_output_device(host: &Host) -> (Device, StreamConfig) {
    let device = host
        .output_devices()
        .expect("Failed to get output devices")
        .find(|candidate| {
            let candidate_name = candidate.to_string().to_uppercase();
            candidate_name.contains("VB") || candidate_name.contains("CABLE")
        })
        .expect("Failed to find VB CABLE. You probably don't have it installed");

    let device_desc = device.description().unwrap();
    println!("Selected: {}", device_desc.name());

    let mut supported_configs_range = device
        .supported_output_configs()
        .expect("Failed to get output configs");

    let config = supported_configs_range
        .next()
        .expect("no supported output config?!")
        .with_max_sample_rate()
        .into();

    (device, config)
}
