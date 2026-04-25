mod cli;

use anyhow::{Context, Result};
use clap::Parser;
use cli::{Cli, Command};
use protostore_core::{
    LocalCache, ObjectBlobStore, ProfileId, TreeId, inspect_tree, materialize_tree, pack_directory,
    repack_tree,
};
use std::{path::Path, str::FromStr, thread, time::Duration};
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Pack { directory, store } => {
            let store = ObjectBlobStore::from_uri(&store)?;
            let runtime = tokio::runtime::Runtime::new().context("creating Tokio runtime")?;
            let tree_id = runtime.block_on(pack_directory(&store, &directory))?;
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
        } => {
            let tree_id = TreeId::from_str(&tree_id).context("parsing tree id")?;
            let store = ObjectBlobStore::from_uri(&store)?;
            let runtime = tokio::runtime::Runtime::new().context("creating Tokio runtime")?;
            let session = protostore_fuse::ProtoStoreFuseBuilder::new(store, tree_id)
                .runtime_handle(runtime.handle().clone())
                .cache(LocalCache::disposable_default())
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
        } => {
            let tree_id = TreeId::from_str(&tree_id).context("parsing tree id")?;
            let store = ObjectBlobStore::from_uri(&store)?;
            let runtime = tokio::runtime::Runtime::new().context("creating Tokio runtime")?;
            runtime.block_on(materialize_tree(
                store,
                tree_id,
                &output_dir,
                LocalCache::disposable_default(),
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
