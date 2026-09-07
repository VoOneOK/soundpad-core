use std::fs;
use std::path::PathBuf;

type StoragePaths = (PathBuf, PathBuf);

pub fn get_storage_paths(qualifier: &str, author: &str, app: &str) -> Result<StoragePaths, String> {
    let path = match directories::ProjectDirs::from(qualifier, author, app) {
        Some(val) => val,
        _ => return Err("Failed to get project directory".to_string()),
    };

    let config_path = path.config_dir().to_path_buf();
    let data_path = path.data_local_dir().to_path_buf();

    fs::create_dir_all(&config_path)
        .map_err(|_| "Couldn't create config directory.".to_string())?;

    fs::create_dir_all(&data_path).map_err(|_| "Couldn't create data directory.".to_string())?;

    Ok((config_path, data_path))
}
