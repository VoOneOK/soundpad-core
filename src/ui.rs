use std::io::{self, Write};

#[derive(Debug)]
pub struct UIDevice {
    pub name: String,
    pub rate: u32,
    pub channels: u16,
}

#[derive(Debug)]
pub struct UIContext {
    pub input: UIDevice,
    pub output: UIDevice,
    pub ratio: f64,
}

pub fn run_ui(context: &UIContext, last_output: &str) -> Result<String, String> {
    println!("{}[2J", 27 as char);

    println!("Commands                                         | State");
    println!(
        "  exit - exits soundpad                          |   Input: {} ({} * {})",
        context.input.name, context.input.channels, context.input.rate,
    );
    println!(
        "  restart - restarts app                         |   Output: {} ({} * {})",
        context.output.name, context.output.channels, context.output.rate,
    );
    println!(
        "  upload x path_to_file - upload sound to slot x |   Ratio: {:.6}",
        context.ratio
    );
    println!("  play x - play sound x                          |   ");
    println!("  list - list sounds                             |   ");

    println!("{}", last_output);
    print!("> ");
    io::stdout().flush().unwrap();

    let mut command = String::new();

    match io::stdin().read_line(&mut command) {
        Ok(_) => {
            return Ok(command);
        }
        Err(e) => {
            return Err(format!("Input error: {}. Try again", e));
        }
    }
}
