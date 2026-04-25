use crate::{
    cache::LocalCache,
    hash::{ChunkId, Hash32, PackId, ProfileId, TreeId},
    pack::{ChunkEntry, Compression, compress_chunk, encode_pack, pack_key, read_chunk_from_pack},
    profile::load_profile,
    reader::TreeReader,
    store::BlobStore,
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::Permissions,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};
use tokio::{fs, io::AsyncReadExt};
use walkdir::WalkDir;

pub const CHUNK_SIZE: usize = 4 * 1024 * 1024;
pub const PACK_TARGET_SIZE: usize = 128 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TreeManifest {
    pub version: u32,
    pub files: Vec<FileNode>,
    pub locations: Vec<ChunkLocationHint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileNode {
    pub path: String,
    pub mode: u32,
    pub size: u64,
    pub chunks: Vec<FileChunkRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileChunkRef {
    pub file_offset: u64,
    pub uncompressed_len: u32,
    pub chunk_id: ChunkId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkLocationHint {
    pub chunk_id: ChunkId,
    pub pack_id: PackId,
    pub compressed_offset: u64,
    pub compressed_len: u32,
    pub uncompressed_len: u32,
}

pub fn tree_id(tree: &TreeManifest) -> Result<TreeId> {
    let json = serde_json::to_vec(tree).context("serializing tree manifest")?;
    Ok(TreeId::new(Hash32::digest(&json)))
}

pub fn tree_key(tree_id: TreeId) -> String {
    format!("trees/{tree_id}.tree")
}

pub async fn load_tree<S: BlobStore>(store: &S, tree_id: TreeId) -> Result<TreeManifest> {
    store.get_json(&tree_key(tree_id)).await
}

pub async fn write_tree<S: BlobStore>(store: &S, tree: &TreeManifest) -> Result<TreeId> {
    let id = tree_id(tree)?;
    store.put_json(&tree_key(id), tree).await?;
    Ok(id)
}

pub async fn pack_directory<S: BlobStore>(store: &S, directory: &Path) -> Result<TreeId> {
    let directory = directory
        .canonicalize()
        .with_context(|| format!("canonicalizing {}", directory.display()))?;
    let mut paths = Vec::new();
    for entry in WalkDir::new(&directory).follow_links(false) {
        let entry = entry.with_context(|| format!("walking {}", directory.display()))?;
        if entry.file_type().is_dir() {
            if entry.file_name() == ".protostore-cache" {
                continue;
            }
            continue;
        }
        if entry.file_type().is_file() {
            paths.push(entry.into_path());
        }
    }
    paths.sort();

    let mut files = Vec::new();
    let mut locations = Vec::new();
    let mut pending = Vec::<(ChunkId, Vec<u8>, u32)>::new();
    let mut pending_compressed_len = 0usize;

    for path in paths {
        let rel = path
            .strip_prefix(&directory)
            .with_context(|| format!("relativizing {}", path.display()))?
            .to_string_lossy()
            .replace('\\', "/");
        let metadata = fs::metadata(&path)
            .await
            .with_context(|| format!("stat {}", path.display()))?;
        let mut file = fs::File::open(&path)
            .await
            .with_context(|| format!("open {}", path.display()))?;
        let mut offset = 0u64;
        let mut refs = Vec::new();
        loop {
            let mut buf = vec![0; CHUNK_SIZE];
            let n = file
                .read(&mut buf)
                .await
                .with_context(|| format!("read {}", path.display()))?;
            if n == 0 {
                break;
            }
            buf.truncate(n);
            let chunk_id = ChunkId::new(Hash32::digest(&buf));
            let compressed = compress_chunk(&buf)?;
            pending_compressed_len += compressed.len();
            refs.push(FileChunkRef {
                file_offset: offset,
                uncompressed_len: u32::try_from(n).context("chunk length overflow")?,
                chunk_id,
            });
            pending.push((chunk_id, compressed, u32::try_from(n).unwrap()));
            offset += n as u64;

            if pending_compressed_len >= PACK_TARGET_SIZE {
                flush_pack(
                    store,
                    &mut pending,
                    &mut pending_compressed_len,
                    &mut locations,
                )
                .await?;
            }
        }
        files.push(FileNode {
            path: rel,
            mode: metadata.permissions().mode(),
            size: metadata.len(),
            chunks: refs,
        });
    }

    flush_pack(
        store,
        &mut pending,
        &mut pending_compressed_len,
        &mut locations,
    )
    .await?;
    files.sort_by(|a, b| a.path.cmp(&b.path));
    locations.sort_by_key(|hint| (hint.pack_id, hint.compressed_offset, hint.chunk_id));
    let tree = TreeManifest {
        version: 1,
        files,
        locations,
    };
    write_tree(store, &tree).await
}

async fn flush_pack<S: BlobStore>(
    store: &S,
    pending: &mut Vec<(ChunkId, Vec<u8>, u32)>,
    pending_compressed_len: &mut usize,
    locations: &mut Vec<ChunkLocationHint>,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let encoded = encode_pack(pending)?;
    store
        .put_bytes(&pack_key(encoded.pack_id), encoded.bytes)
        .await?;
    for entry in encoded.index.chunks {
        locations.push(ChunkLocationHint {
            chunk_id: entry.chunk_id,
            pack_id: encoded.pack_id,
            compressed_offset: entry.compressed_offset,
            compressed_len: entry.compressed_len,
            uncompressed_len: entry.uncompressed_len,
        });
    }
    pending.clear();
    *pending_compressed_len = 0;
    Ok(())
}

pub fn inspect_tree(tree_id: TreeId, tree: &TreeManifest) -> String {
    let file_count = tree.files.len();
    let chunk_count = tree.locations.len();
    let total_size: u64 = tree.files.iter().map(|file| file.size).sum();
    format!(
        "tree {tree_id}\nfiles: {file_count}\nchunks: {chunk_count}\nlogical bytes: {total_size}"
    )
}

pub async fn materialize_tree<S: BlobStore>(
    store: S,
    tree_id: TreeId,
    output_dir: &Path,
    cache: LocalCache,
) -> Result<()> {
    let reader = TreeReader::open(store, tree_id, cache).await?;
    reader.materialize(output_dir).await
}

pub async fn repack_tree<S: BlobStore>(
    store: &S,
    tree_id: TreeId,
    profile_id: ProfileId,
) -> Result<TreeId> {
    let tree = load_tree(store, tree_id).await?;
    let profile = load_profile(store, profile_id).await?;
    let read_times: HashMap<ChunkId, u128> = profile
        .events
        .into_iter()
        .map(|event| (event.chunk_id, event.first_read_unix_ns))
        .collect();
    let location_by_chunk: HashMap<ChunkId, ChunkLocationHint> = tree
        .locations
        .iter()
        .map(|hint| (hint.chunk_id, hint.clone()))
        .collect();

    let mut chunk_order = Vec::<(ChunkId, String, u64)>::new();
    let mut seen = HashSet::new();
    for file in &tree.files {
        for chunk in &file.chunks {
            if seen.insert(chunk.chunk_id) {
                chunk_order.push((chunk.chunk_id, file.path.clone(), chunk.file_offset));
            }
        }
    }
    chunk_order.sort_by(|a, b| {
        let at = read_times.get(&a.0).copied();
        let bt = read_times.get(&b.0).copied();
        at.cmp(&bt)
            .then_with(|| a.1.cmp(&b.1))
            .then_with(|| a.2.cmp(&b.2))
            .then_with(|| a.0.cmp(&b.0))
    });

    let mut new_locations = Vec::new();
    let mut pending = Vec::<(ChunkId, Vec<u8>, u32)>::new();
    let mut pending_len = 0usize;
    for (chunk_id, _, _) in chunk_order {
        let hint = location_by_chunk
            .get(&chunk_id)
            .with_context(|| format!("missing location for chunk {chunk_id}"))?;
        let entry = ChunkEntry {
            chunk_id,
            compressed_offset: hint.compressed_offset,
            compressed_len: hint.compressed_len,
            uncompressed_len: hint.uncompressed_len,
            compression: Compression::Zstd,
        };
        let decompressed = read_chunk_from_pack(store, hint.pack_id, &entry).await?;
        ensure!(
            ChunkId::new(Hash32::digest(&decompressed)) == chunk_id,
            "chunk hash mismatch while repacking {chunk_id}"
        );
        let compressed = compress_chunk(&decompressed)?;
        pending_len += compressed.len();
        pending.push((chunk_id, compressed, hint.uncompressed_len));
        if pending_len >= PACK_TARGET_SIZE {
            flush_pack(store, &mut pending, &mut pending_len, &mut new_locations).await?;
        }
    }
    flush_pack(store, &mut pending, &mut pending_len, &mut new_locations).await?;
    let mut new_tree = tree;
    new_locations.sort_by_key(|hint| (hint.pack_id, hint.compressed_offset, hint.chunk_id));
    new_tree.locations = new_locations;
    write_tree(store, &new_tree).await
}

pub fn file_map(tree: &TreeManifest) -> BTreeMap<String, FileNode> {
    tree.files
        .iter()
        .map(|file| (file.path.clone(), file.clone()))
        .collect()
}

pub fn location_map(tree: &TreeManifest) -> HashMap<ChunkId, ChunkLocationHint> {
    tree.locations
        .iter()
        .map(|hint| (hint.chunk_id, hint.clone()))
        .collect()
}

pub async fn write_file_mode(path: &Path, mode: u32) -> Result<()> {
    let permissions = Permissions::from_mode(mode);
    fs::set_permissions(path, permissions)
        .await
        .with_context(|| format!("setting permissions on {}", path.display()))
}

pub fn safe_output_path(output_dir: &Path, rel: &str) -> Result<PathBuf> {
    let rel_path = Path::new(rel);
    if rel_path.is_absolute()
        || rel_path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        bail!("unsafe tree path {rel}");
    }
    Ok(output_dir.join(rel_path))
}
