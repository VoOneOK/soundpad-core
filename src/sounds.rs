use std::{path::Path, path::PathBuf, process::Command};

pub fn upload_sound(id: &str, file_path_str: &str, target_dir: &Path) -> Result<(), String> {
    let file_path = PathBuf::from(file_path_str);
    let file_exist = file_path.try_exists().unwrap_or(false);

    if !file_exist {
        return Err("File doesn't exist".to_string());
    }

    let target_path = target_dir.join(format!("{id}.wav"));

    Command::new("ffmpeg")
        .args([
            "-loglevel",
            "quiet",
            "-i",
            &file_path_str,
            "-ar",
            "48000",
            "-ac",
            "2",
            "-c:a",
            "pcm_f32le",
            target_path.to_str().unwrap(),
        ])
        .output()
        .map_err(|err: std::io::Error| format!("Failed to call ffmpeg: {}", err))?;

    Ok(())
}
