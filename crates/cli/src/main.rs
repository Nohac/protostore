mod cli;

use anyhow::{Context, Result};
use clap::Parser;
use cli::{Cli, Command};
use protostore_core::{
    LocalCache, ObjectBlobStore, ProfileId, TreeId, inspect_tree, materialize_tree, pack_directory,
    repack_tree,
};
use std::str::FromStr;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Pack { directory, store } => {
            let store = ObjectBlobStore::from_uri(&store)?;
            let tree_id = pack_directory(&store, &directory).await?;
            println!("{tree_id}");
        }
        Command::Inspect { tree_id, store } => {
            let tree_id = TreeId::from_str(&tree_id).context("parsing tree id")?;
            let store = ObjectBlobStore::from_uri(&store)?;
            let tree = protostore_core::tree::load_tree(&store, tree_id).await?;
            println!("{}", inspect_tree(tree_id, &tree));
        }
        Command::Mount {
            tree_id,
            mountpoint,
            store,
        } => {
            let tree_id = TreeId::from_str(&tree_id).context("parsing tree id")?;
            let store = ObjectBlobStore::from_uri(&store)?;
            protostore_fuse::mount_readonly(
                store,
                tree_id,
                &mountpoint,
                LocalCache::disposable_default(),
            )?;
        }
        Command::Materialize {
            tree_id,
            output_dir,
            store,
        } => {
            let tree_id = TreeId::from_str(&tree_id).context("parsing tree id")?;
            let store = ObjectBlobStore::from_uri(&store)?;
            materialize_tree(
                store,
                tree_id,
                &output_dir,
                LocalCache::disposable_default(),
            )
            .await?;
        }
        Command::Repack {
            tree_id,
            profile,
            store,
        } => {
            let tree_id = TreeId::from_str(&tree_id).context("parsing tree id")?;
            let profile_id = ProfileId::from_str(&profile).context("parsing profile id")?;
            let store = ObjectBlobStore::from_uri(&store)?;
            let new_tree_id = repack_tree(&store, tree_id, profile_id).await?;
            println!("{new_tree_id}");
        }
    }
    Ok(())
}
