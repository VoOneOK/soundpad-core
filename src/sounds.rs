use std::{path::PathBuf, process::Command};

pub fn upload_sound(id: &str, file_path_str: &str, target_dir: PathBuf) -> Result<String, String> {
    let file_path = PathBuf::from(file_path_str);
    let file_exist = file_path.try_exists().unwrap_or(false);

    if !file_exist {
        return Err("File doesn't exist".to_string());
    }

    let target_path = target_dir.clone().join(format!("{id}.wav"));

    Command::new("ffmpeg")
        .args([
            "-loglevel",
            "quiet",
            "-i",
            &file_path_str,
            "-ar",
            "48000",
            "-ac",
            target_path.to_str().unwrap(),
        ])
        .output()
        .map_err(|err: std::io::Error| format!("Failed to call ffmpeg: {}", err))?;

    Ok("Successfully uploaded sound".to_string())
}
