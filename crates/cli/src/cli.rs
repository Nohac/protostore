use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "protostore")]
#[command(about = "Immutable object-store-backed tree store")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Pack {
        directory: PathBuf,
        #[arg(long)]
        store: String,
        #[arg(long, value_parser = crate::parse_size)]
        chunk_size: Option<usize>,
        #[arg(
            long,
            help = "Number of parallel pack workers. Defaults to available CPU threads."
        )]
        pack_workers: Option<usize>,
        #[arg(
            long,
            help = "Physical pack key prefix under packs/. Defaults to a UUIDv7."
        )]
        key: Option<String>,
    },
    Inspect {
        key: String,
        #[arg(long)]
        store: String,
    },
    Mount {
        key: String,
        mountpoint: PathBuf,
        #[arg(long)]
        store: String,
        #[arg(long, value_parser = crate::parse_size)]
        min_remote_read: Option<usize>,
        #[arg(long, value_parser = crate::parse_size)]
        target_coalesce: Option<usize>,
    },
    Materialize {
        key: String,
        output_dir: PathBuf,
        #[arg(long)]
        store: String,
        #[arg(long, value_parser = crate::parse_size)]
        min_remote_read: Option<usize>,
        #[arg(long, value_parser = crate::parse_size)]
        target_coalesce: Option<usize>,
    },
    Repack {
        key: String,
        #[arg(long)]
        profile: String,
        #[arg(long)]
        store: String,
    },
}
