use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuffersSettings {
    /// Input ring buffer length in seconds (0.2 for 200 ms)
    pub input_sec: f64,
    /// Output ring buffer length in seconds (0.2 for 200 ms)
    pub output_sec: f64,
    /// Clips and loaded clips ring buffer length in seconds (0.2 for 200 ms)
    pub clips_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MicResamplerSettings {
    /// Samples to collect and process by rubato at a time
    pub chunk_size: usize,
    /// Delay to let input buffer fill before any resampling
    pub start_delay_ms: u64,
    /// Delay before retrying to get samples if currently not enough
    pub input_poll_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClipsResamplerSettings {
    /// Samples to collect and process by rubato at a time
    pub chunk_size: usize,
    /// What to assume when unknown how full buffer is
    pub unknown_buffer_fullness: f64,
    /// Delay to let input buffer fill before any resampling
    pub start_delay_ms: u64,
    /// How often to check if new sound started playing when idle
    pub clip_poll_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoundLoaderSettings {
    /// How often to check if new sound started playing when idle
    pub clip_poll_ms: u64,
    /// How often to check if new samples needed when not idle
    pub read_poll_ms: u64,
    /// How often to check if buffer clear happen
    pub clear_poll_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSettings {
    /// Channels amount of stored wav sounds
    pub sounds_channels: u32,
    /// Rate of stored wav sounds
    pub sounds_rate: u64,
}
