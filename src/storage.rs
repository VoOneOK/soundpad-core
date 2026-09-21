use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct ConfigPaths {
    pub root_dir: PathBuf,
    pub sounds: PathBuf,
}

#[derive(Debug)]
pub struct DataPaths {
    pub root_dir: PathBuf,
    pub sounds_dir: PathBuf,
}

#[derive(Debug)]
pub struct StoragePaths {
    pub config: ConfigPaths,
    pub data: DataPaths,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Sound {
    pub uuid: String,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SoundsConfig {
    pub sounds: Vec<Sound>,
}

#[derive(Debug)]
pub enum Clip {
    Preloaded(Vec<f32>),
    Partial {
        head: Vec<f32>,
        path: PathBuf,
        samples_read: usize,
    },
    NotLoaded {
        error: String,
        path: PathBuf,
    },
}

pub fn storage_paths(qualifier: &str, author: &str, app: &str) -> Result<StoragePaths, String> {
    let app_path = directories::ProjectDirs::from(qualifier, author, app)
        .ok_or_else(|| "Failed to get project directory".to_string())?;

    let config_dir = app_path.config_dir().to_path_buf();
    let sounds_config = config_dir.join("sounds.json");

    let data_dir = app_path.data_local_dir().to_path_buf();
    let sounds_data_dir = data_dir.join("sounds");

    fs::create_dir_all(&config_dir).map_err(|_| "Couldn't create config directory")?;
    fs::create_dir_all(&sounds_data_dir).map_err(|_| "Couldn't create data directory")?;

    Ok(StoragePaths {
        config: ConfigPaths {
            root_dir: config_dir,
            sounds: sounds_config,
        },
        data: DataPaths {
            root_dir: data_dir,
            sounds_dir: sounds_data_dir,
        },
    })
}

pub fn read_sounds_config(sounds_config: &PathBuf) -> Result<SoundsConfig, String> {
    let file_exist = sounds_config.try_exists().unwrap_or(false);

    if !file_exist {
        match fs::write(sounds_config, r#"{ "sounds": [] }"#) {
            Ok(_) => {
                let empty_sounds_config = SoundsConfig { sounds: Vec::new() };
                return Ok(empty_sounds_config);
            }
            _ => {
                return Err("Failed to write to sounds config".into());
            }
        }
    }

    let sounds_json =
        fs::read_to_string(sounds_config).map_err(|_| "Failed to read sounds config")?;

    let sounds_data: SoundsConfig =
        serde_json::from_str(&sounds_json).map_err(|e| e.to_string())?;

    Ok(sounds_data)
}

pub fn write_sounds_config(path: &Path, new_config: &SoundsConfig) -> Result<(), String> {
    let new_json = serde_json::to_string_pretty(new_config)
        .map_err(|err| format!("Failed to generate sounds config: {err}"))?;

    fs::write(path, new_json).map_err(|err| format!("Failed to write sounds config: {err}"))?;

    Ok(())
}

pub fn verify_and_load_sounds(
    sounds: &mut Vec<Sound>,
    sounds_dir: &Path,
    max_samples: usize,
) -> HashMap<String, Clip> {
    let mut preloaded: HashMap<String, Clip> = HashMap::new();

    sounds.retain(|sound| {
        let sound_path = sounds_dir.join(format!("{}.wav", sound.uuid));
        if !sound_path.try_exists().unwrap_or(false) {
            return false;
        }

        let mut reader = hound::WavReader::open(&sound_path).unwrap();
        let samples: Vec<f32> = match reader
            .samples::<f32>()
            .take(max_samples)
            .collect::<Result<_, _>>()
        {
            Ok(val) => val,
            Err(err) => {
                preloaded.insert(
                    sound.uuid.clone(),
                    Clip::NotLoaded {
                        error: format!("Failed to preload: {}", err),
                        path: sound_path,
                    },
                );
                return true;
            }
        };

        let samples_read = samples.len();

        preloaded.insert(
            sound.uuid.clone(),
            if samples_read >= max_samples {
                Clip::Partial {
                    head: samples,
                    path: sound_path,
                    samples_read,
                }
            } else {
                Clip::Preloaded(samples)
            },
        );

        true
    });

    preloaded
}
