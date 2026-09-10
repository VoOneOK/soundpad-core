use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize)]
pub struct Sound {
    uuid: String,
    name: String,
    file_path: String, // Keybinds later
}

#[derive(Serialize, Deserialize)]
pub struct SoundsConfig {
    sounds: Vec<Sound>,
}

type StoragePaths = (PathBuf, PathBuf);

pub fn get_storage_paths(qualifier: &str, author: &str, app: &str) -> Result<StoragePaths, String> {
    let path = match directories::ProjectDirs::from(qualifier, author, app) {
        Some(val) => val,
        _ => return Err("Failed to get project directory".to_string()),
    };

    let config_path = path.config_dir().to_path_buf();
    let data_path = path.data_local_dir().to_path_buf();

    fs::create_dir_all(&config_path).map_err(|_| "Couldn't create config directory.")?;
    fs::create_dir_all(&data_path).map_err(|_| "Couldn't create data directory.")?;

    Ok((config_path, data_path))
}

pub fn read_sounds_config(config_path: &PathBuf) -> Result<SoundsConfig, String> {
    let sounds_config = Path::new(config_path).join("sounds.json");
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
