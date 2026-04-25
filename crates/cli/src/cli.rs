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
    },
    Inspect {
        tree_id: String,
        #[arg(long)]
        store: String,
    },
    Mount {
        tree_id: String,
        mountpoint: PathBuf,
        #[arg(long)]
        store: String,
    },
    Materialize {
        tree_id: String,
        output_dir: PathBuf,
        #[arg(long)]
        store: String,
    },
    Repack {
        tree_id: String,
        #[arg(long)]
        profile: String,
        #[arg(long)]
        store: String,
    },
}
