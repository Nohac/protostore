mod cli;

use anyhow::{Context, Result};
use clap::Parser;
use cli::{Cli, Command};
use protostore_core::{
    LocalCache, ObjectBlobStore, PackConfig, ProfileId, ReadConfig, inspect_tree,
    materialize_tree_with_config, pack_directory_with_config, repack_tree,
};
use std::{path::Path, str::FromStr, thread, time::Duration};
use tokio::runtime::Handle;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let runtime = tokio::runtime::Runtime::new().context("creating Tokio runtime")?;
    runtime.block_on(run(cli))
}

async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Pack {
            directory,
            store,
            chunk_size,
            compression_level,
            pack_workers,
            key,
        } => {
            let store = ObjectBlobStore::from_uri(&store).await?;
            let pack_config = pack_config(chunk_size, compression_level, pack_workers, key)?;
            let packed = pack_directory_with_config(&store, &directory, pack_config).await?;
            println!("{}", packed.key);
        }
        Command::Inspect { key, store } => {
            let store = ObjectBlobStore::from_uri(&store).await?;
            let tree = protostore_core::tree::load_tree(&store, &key).await?;
            println!("{}", inspect_tree(tree.tree_id, &tree));
        }
        Command::Mount {
            key,
            mountpoint,
            store,
            min_remote_read,
            target_coalesce,
            read_ahead_chunks,
            read_ahead_bytes,
            read_ahead_concurrency,
        } => {
            let read_config = read_config(
                min_remote_read,
                target_coalesce,
                read_ahead_chunks,
                read_ahead_bytes,
                read_ahead_concurrency,
            )?;
            let store = ObjectBlobStore::from_uri(&store).await?;
            let session = protostore_fuse::ProtoStoreFuseBuilder::new(store, key)
                .runtime_handle(Handle::current())
                .cache(LocalCache::disposable_default())
                .read_config(read_config)
                .spawn_async(&mountpoint)
                .await?;
            eprintln!("mounted {}. Press Ctrl-C to unmount.", mountpoint.display());
            tokio::signal::ctrl_c()
                .await
                .context("waiting for Ctrl-C")?;
            eprintln!("unmounting {}", mountpoint.display());
            drop(session);
            thread::sleep(Duration::from_millis(100));
            if is_mounted(&mountpoint)? {
                eprintln!(
                    "warning: {} is still mounted, likely because a process has it open",
                    mountpoint.display()
                );
                eprintln!(
                    "close shells/files under the mountpoint, then run: fusermount3 -u {}",
                    mountpoint.display()
                );
                eprintln!(
                    "for lazy detach on Linux, run: fusermount3 -uz {}",
                    mountpoint.display()
                );
            }
        }
        Command::Materialize {
            key,
            output_dir,
            store,
            min_remote_read,
            target_coalesce,
            read_ahead_chunks,
            read_ahead_bytes,
            read_ahead_concurrency,
        } => {
            let read_config = read_config(
                min_remote_read,
                target_coalesce,
                read_ahead_chunks,
                read_ahead_bytes,
                read_ahead_concurrency,
            )?;
            let store = ObjectBlobStore::from_uri(&store).await?;
            materialize_tree_with_config(
                store,
                &key,
                &output_dir,
                LocalCache::disposable_default(),
                read_config,
            )
            .await?;
        }
        Command::Repack {
            key,
            profile,
            store,
        } => {
            let profile_id = ProfileId::from_str(&profile).context("parsing profile id")?;
            let store = ObjectBlobStore::from_uri(&store).await?;
            let packed = repack_tree(&store, &key, profile_id).await?;
            println!("{}", packed.key);
        }
    }
    Ok(())
}

fn is_mounted(mountpoint: &Path) -> Result<bool> {
    let mountpoint = mountpoint
        .canonicalize()
        .with_context(|| format!("canonicalizing mountpoint {}", mountpoint.display()))?;
    let mountpoint = mountpoint.to_string_lossy();
    let mountinfo =
        std::fs::read_to_string("/proc/self/mountinfo").context("reading /proc/self/mountinfo")?;
    Ok(mountinfo.lines().any(|line| {
        line.split_whitespace()
            .nth(4)
            .map(unescape_mountinfo_path)
            .as_deref()
            == Some(mountpoint.as_ref())
    }))
}

fn unescape_mountinfo_path(path: &str) -> String {
    path.replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}

fn pack_config(
    chunk_size: Option<usize>,
    compression_level: i32,
    pack_workers: Option<usize>,
    key: Option<String>,
) -> Result<PackConfig> {
    let defaults = PackConfig::default();
    let chunk_size = chunk_size.unwrap_or(defaults.chunk_size);
    PackConfig {
        chunk_size,
        compression_level,
        pack_workers: pack_workers.unwrap_or(defaults.pack_workers),
        key: key.unwrap_or(defaults.key),
    }
    .validate()
}

fn read_config(
    min_remote_read: Option<usize>,
    target_coalesce: Option<usize>,
    read_ahead_chunks: Option<usize>,
    read_ahead_bytes: Option<usize>,
    read_ahead_concurrency: Option<usize>,
) -> Result<ReadConfig> {
    let defaults = ReadConfig::default();
    let min_remote_read = min_remote_read.unwrap_or(defaults.min_remote_read);
    ReadConfig {
        min_remote_read,
        target_coalesce: target_coalesce.unwrap_or(defaults.target_coalesce.max(min_remote_read)),
        read_ahead_chunks: read_ahead_chunks.unwrap_or(defaults.read_ahead_chunks),
        read_ahead_bytes: read_ahead_bytes.unwrap_or(defaults.read_ahead_bytes),
        read_ahead_concurrency: read_ahead_concurrency.unwrap_or(defaults.read_ahead_concurrency),
    }
    .validate()
}

fn parse_size(value: &str) -> std::result::Result<usize, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("size cannot be empty".to_string());
    }
    let split_at = value
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(value.len());
    let (digits, suffix) = value.split_at(split_at);
    if digits.is_empty() {
        return Err(format!("size must start with digits: {value}"));
    }
    let number: usize = digits
        .parse()
        .map_err(|err| format!("invalid size number {digits}: {err}"))?;
    let multiplier = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1usize,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        other => return Err(format!("unsupported size suffix: {other}")),
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| format!("size is too large: {value}"))
}
