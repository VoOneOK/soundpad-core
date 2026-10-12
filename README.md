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
- [hound](https://crates.io/crates/hound) for reading wavs

## Under the hood

This soundpad is multiple threads working together in sync:

1. Main: preloads beginnings of sounds, asks which microphone to use, handles ui, does stuff it shouldn't (not critical, to be fixed)
2. `Cpal`'s input stream gets microphone samples
3. Microphone resampling and channel conversion
4. Sounds loading: dynamically loads sounds from the disk to ram just in time, keeps ram usage low
5. Sounds resampling and channel conversion (reads preloaded samples first, then dynamically loaded ones)
6. `Cpal`'s output stream merges microphone and sounds and passes to `vb audio`

## WIP!!!

This soundpad already has basic soundpad functionality: microphone switch, sound playing. My casual tests show instant reaction to play command. Though still a lot of necessary features missing. Check the Roadmap for the list of planned features

## Roadmap

### Step 1 - finish cli prototype

- crud operations on sounds
- global volume control, as well as per sound volume ones
- add check for ffmpeg installed on startup and improve the check for vb audio
- ability to make changes to settings from ui and hot reload where possible
- sound uploading in a separate thread
- skip resampling when input and output configs match (rates, channels, types)
- make few silent fails more noticeable (ones those maybe should not be silent)
- add config backups (if read of config failed, attempt to use last stable version)
- support non-f32 configs for input and output (not sure if needed)
- self hear for sounds (yeah, right now only your friends hear sounds, poor guys 😭 (or poor you?) )

### Step 2 - move from an app to an engine

This repo might get converted to monorepo with 3 main parts: engine, cli wrapper, gui wrapper (probably `tauri`)

- make sure no unsafe unwraps are left
- self healing restarts of failed threads
- errors as enum values instead of strings
- make engine independent, so anyone can write ui for it using api (or vise versa: write another engine for existing ui wrapper, e.g. another os)

### Step 3 - GUI part

That is when soundpad will become not a prototype but a working project.

### While these steps happen

- fix potential under/over-runs
- check for memory leaks on multi hour test with different conditions

## Building

Project isn't in a state when releases make sense. Though if you still wish to use it:

```
cargo build --release
```

## Contribution

Any help by anyone is welcome!

## License: MIT

More here: [LICENSE.md](./LICENSE.md)
