mod cli;

use anyhow::{Context, Result};
use clap::Parser;
use cli::{Cli, Command};
use protostore_core::{
    LocalCache, ObjectBlobStore, PackConfig, ProfileId, ReadConfig, TreeId, inspect_tree,
    materialize_tree_with_config, pack_directory_with_config, repack_tree,
};
use std::{path::Path, str::FromStr, thread, time::Duration};
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Pack {
            directory,
            store,
            chunk_size,
            pack_target_size,
            pack_workers,
        } => {
            let store = ObjectBlobStore::from_uri(&store)?;
            let pack_config = pack_config(chunk_size, pack_target_size, pack_workers)?;
            let runtime = tokio::runtime::Runtime::new().context("creating Tokio runtime")?;
            let tree_id =
                runtime.block_on(pack_directory_with_config(&store, &directory, pack_config))?;
            println!("{tree_id}");
        }
        Command::Inspect { tree_id, store } => {
            let tree_id = TreeId::from_str(&tree_id).context("parsing tree id")?;
            let store = ObjectBlobStore::from_uri(&store)?;
            let runtime = tokio::runtime::Runtime::new().context("creating Tokio runtime")?;
            let tree = runtime.block_on(protostore_core::tree::load_tree(&store, tree_id))?;
            println!("{}", inspect_tree(tree_id, &tree));
        }
        Command::Mount {
            tree_id,
            mountpoint,
            store,
            min_remote_read,
            target_coalesce,
        } => {
            let tree_id = TreeId::from_str(&tree_id).context("parsing tree id")?;
            let store = ObjectBlobStore::from_uri(&store)?;
            let read_config = read_config(min_remote_read, target_coalesce)?;
            let runtime = tokio::runtime::Runtime::new().context("creating Tokio runtime")?;
            let session = protostore_fuse::ProtoStoreFuseBuilder::new(store, tree_id)
                .runtime_handle(runtime.handle().clone())
                .cache(LocalCache::disposable_default())
                .read_config(read_config)
                .spawn(&mountpoint)?;
            eprintln!("mounted {}. Press Ctrl-C to unmount.", mountpoint.display());
            runtime
                .block_on(tokio::signal::ctrl_c())
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
            tree_id,
            output_dir,
            store,
            min_remote_read,
            target_coalesce,
        } => {
            let tree_id = TreeId::from_str(&tree_id).context("parsing tree id")?;
            let store = ObjectBlobStore::from_uri(&store)?;
            let read_config = read_config(min_remote_read, target_coalesce)?;
            let runtime = tokio::runtime::Runtime::new().context("creating Tokio runtime")?;
            runtime.block_on(materialize_tree_with_config(
                store,
                tree_id,
                &output_dir,
                LocalCache::disposable_default(),
                read_config,
            ))?;
        }
        Command::Repack {
            tree_id,
            profile,
            store,
        } => {
            let tree_id = TreeId::from_str(&tree_id).context("parsing tree id")?;
            let profile_id = ProfileId::from_str(&profile).context("parsing profile id")?;
            let store = ObjectBlobStore::from_uri(&store)?;
            let runtime = tokio::runtime::Runtime::new().context("creating Tokio runtime")?;
            let new_tree_id = runtime.block_on(repack_tree(&store, tree_id, profile_id))?;
            println!("{new_tree_id}");
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
    pack_target_size: Option<usize>,
    pack_workers: Option<usize>,
) -> Result<PackConfig> {
    let defaults = PackConfig::default();
    let chunk_size = chunk_size.unwrap_or(defaults.chunk_size);
    PackConfig {
        chunk_size,
        pack_target_size: pack_target_size.unwrap_or(defaults.pack_target_size.max(chunk_size)),
        pack_workers: pack_workers.unwrap_or(defaults.pack_workers),
    }
    .validate()
}

fn read_config(
    min_remote_read: Option<usize>,
    target_coalesce: Option<usize>,
) -> Result<ReadConfig> {
    let defaults = ReadConfig::default();
    let min_remote_read = min_remote_read.unwrap_or(defaults.min_remote_read);
    ReadConfig {
        min_remote_read,
        target_coalesce: target_coalesce.unwrap_or(defaults.target_coalesce.max(min_remote_read)),
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
