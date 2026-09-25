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

Soundpad already can play sounds, those are less than 5 mb (~13.5 seconds of audio). Though lots of important stuff is missing. Check the TODO section for the list of features to be implemented

## TODO (first = highest priority right now)

- implement dynamic loading of large sounds (over 5 mb) in a separate thread
- add check for ffmpeg installed on startup and improve the check for vb audio
- per sound volume control
- ability to make changes from ui and hot reload where possible
- sound uploading in a separate thread (might not be implemented here but definitely will in gui version)
- skip resampling when input and output configs match (rates, channels, types)
- make few silent fails more noticeable (ones those maybe should not be silent)
- add config backups (if read of config failed, attempt to use last stable version)
- support non-f32 configs for input and output
- fix potential under/over-runs
- check for memory leaks on multi hour test with different conditions

If you see a todo at the bottom of the list, it does not mean it is not important. Some stuff is pointless right now but will become relevant in the future

## Contribution

Any help by anyone is welcome!

## License: MIT

More here: [LICENSE.md](./LICENSE.md)
