use clap::Parser;
use serde::Deserialize;
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::Instant,
    io::{BufRead, BufReader},
};

#[derive(Parser)]
#[command(author, version, about)]
struct Args {
    /// Diretório PCAP
    #[arg(short = 'd')]
    dir: String,

    /// Categoria ASTERIX
    #[arg(short = 'c')]
    category: String,

    /// Adicionar timestamp
    #[arg(long = "ts")]
    timestamp: bool,

    /// Jobs paralelos
    #[arg(short = 'j', default_value_t = num_cpus::get())]
    workers: usize,
}

#[derive(Debug, Deserialize)]
struct Config {
    tshark: Tshark,
    categories: std::collections::HashMap<String, Vec<Field>>,
}

#[derive(Debug, Deserialize)]
struct Tshark {
    path: String,
    parameters: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct Field {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Value")]
    value: String,
}

fn load_config(path: &str) -> anyhow::Result<Config> {
    let data = fs::read_to_string(path)?;
    Ok(toml::from_str(&data)?)
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let cfg = Arc::new(load_config("config.toml")?);

    // Carrega todos os arquivos do diretório
    let mut files: Vec<PathBuf> = fs::read_dir(&args.dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .collect();

    if files.is_empty() {
        println!("⚠️ Nenhum PCAP encontrado");
        return Ok(());
    }

    // Inverte o vetor para usar pop() (mais eficiente que remove(0))
    files.reverse();

    let jobs = Arc::new(Mutex::new(files));
    let start = Instant::now();
    let mut handles = Vec::new();

    for _ in 0..args.workers {
        let jobs = Arc::clone(&jobs);
        let cfg = Arc::clone(&cfg);
        let category = args.category.clone();
        let timestamp = args.timestamp;

        handles.push(thread::spawn(move || {
            loop {
                // Pega 1 arquivo do final do vetor
                let file_opt = {
                    let mut files = jobs.lock().unwrap();
                    files.pop()
                };

                match file_opt {
                    Some(file) => {
                        if let Err(e) = process_file(&cfg, &file, &category, timestamp) {
                            eprintln!("❌ {:?}: {}", file, e);
                        }
                    }
                    None => break, // Não tem mais arquivos
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    println!(
        "\n✔ Processamento finalizado em {:.2}s",
        start.elapsed().as_secs_f64()
    );

    Ok(())
}

fn process_file(
    cfg: &Config,
    filename: &Path,
    category: &str,
    timestamp: bool,
) -> anyhow::Result<()> {
    let start = Instant::now();

    let fields = cfg
        .categories
        .get(category)
        .ok_or_else(|| anyhow::anyhow!("CAT {} não encontrada", category))?;

    let outfile = filename
        .file_stem()
        .unwrap()
        .to_string_lossy()
        .to_string()
        + ".csv";

    let mut headers = Vec::new();
    let mut args = Vec::new();

    args.push("-r".into());
    args.push(filename.to_string_lossy().into());
    args.extend(cfg.tshark.parameters.clone());
    args.push("-Y".into());
    args.push(format!("asterix.category=={}", category));

    if timestamp {
        headers.push("TIMESTAMP".into());
        args.push("-e".into());
        args.push("frame.time_epoch".into());
    }

    for f in fields {
        headers.push(f.key.clone());
        args.push("-e".into());
        args.push(f.value.clone());
    }

    let mut child = Command::new(&cfg.tshark.path)
        .args(&args)
        .stdout(Stdio::piped())
        .spawn()?;

    let stdout = child.stdout.take().unwrap();
    let reader = BufReader::new(stdout);

    let mut writer = csv::WriterBuilder::new()
        .delimiter(b';')
        .from_path(&outfile)?;

    writer.write_record(&headers)?;

    for line in reader.lines() {
        let line = line?;
        let record: Vec<&str> = line.split(';').collect();
        writer.write_record(&record)?;
    }

    writer.flush()?;
    child.wait()?;

    println!(
        "✔ {} → {} ({:.2}s)",
        filename.file_name().unwrap().to_string_lossy(),
        outfile,
        start.elapsed().as_secs_f64()
    );

    Ok(())
}
