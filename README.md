# Vooneok's Open Soundpad Core

## What is this

This is a bare CLI implementation for my future project: "**Open Soundpad**" (coming soon!)

## Stack

Written fully in `Rust`. It uses

- [VB Audio](https://vb-audio.com/Cable/) for fake microphone simulation on Windows
- [FFMPEG](https://vb-audio.com/Cable/) for copying sounds as wavs
- [cpal](https://crates.io/crates/cpal) for capturing input and streaming output
- [rubato](https://crates.io/crates/rubato) for audio resampling
- [ringbuf](https://crates.io/crates/ringbuf) for ring buffering samples in different stages
- [directories](https://crates.io/crates/directories) for getting config and data directories
- [serde and serde_json](https://crates.io/crates/serde) for parsing json configs
- [uuid](https://crates.io/crates/uuid) for generating uuids for sounds

## Under the hood

1. `cpal` captures input on selected microphone
2. Samples get sent to first ring buffer
3. Loop collects chunks of samples, those `rubato` then resamples
4. Channel conversion (support for 1:1 ratio, mono to stereo and stereo to mono)
5. Samples get into second ring buffer
6. Output stream pops samples and apply them to the `cpal` stream

## WIP!!!

Project lacks basic soundpad features. It only sends audio stream from microphone to VB Audio **for now**. More core updates coming soon.

## TODO

- support non-f32 configs for input and output
- add sound functionality (upload, convert, load into memory when needed, mix with input)
- fix potential under/over-runs
- check for memory leaks on multi hour test with different conditions

## Contribution

Any help by anyone is welcome!

## License: MIT

More here: [LICENSE.md](./LICENSE.md)
