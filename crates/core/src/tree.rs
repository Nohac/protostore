use crate::{
    cache::LocalCache,
    hash::{ChunkId, Hash32, LayoutId, ProfileId, TreeId},
    pack::{ChunkEntry, Compression, compress_chunk, read_chunk_from_pack_key, stream_pack_to_key},
    profile::load_profile,
    reader::{ReadConfig, TreeReader},
    store::BlobStore,
};
use anyhow::{Context, Result, bail, ensure};
use async_channel::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::Permissions,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};
use tokio::fs;
use tracing::{debug, warn};
use walkdir::WalkDir;

pub const DEFAULT_CHUNK_SIZE: usize = 16 * 1024 * 1024;
pub const PACK_TARGET_SIZE: usize = 128 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackConfig {
    pub chunk_size: usize,
    pub pack_target_size: usize,
    pub pack_workers: usize,
    pub pack_key_prefix: String,
}

impl Default for PackConfig {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
            pack_target_size: PACK_TARGET_SIZE,
            pack_workers: default_pack_workers(),
            pack_key_prefix: generated_pack_key_prefix(),
        }
    }
}

impl PackConfig {
    pub fn validate(self) -> Result<Self> {
        ensure!(self.chunk_size > 0, "chunk size must be greater than zero");
        ensure!(
            self.chunk_size <= u32::MAX as usize,
            "chunk size must fit in u32"
        );
        ensure!(
            self.pack_target_size >= self.chunk_size,
            "pack target size must be at least chunk size"
        );
        ensure!(
            self.pack_workers > 0,
            "pack workers must be greater than zero"
        );
        ensure!(
            valid_pack_key_prefix(&self.pack_key_prefix),
            "pack key must be relative and must not contain empty path segments or '..'"
        );
        Ok(self)
    }
}

fn generated_pack_key_prefix() -> String {
    uuid::Uuid::now_v7().to_string()
}

fn valid_pack_key_prefix(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('/')
        && value
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

fn default_pack_workers() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4)
        .max(1)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TreeManifest {
    pub version: u32,
    pub files: Vec<FileNode>,
    pub layout_id: LayoutId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalTreeManifest {
    pub version: u32,
    pub files: Vec<FileNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LayoutManifest {
    pub version: u32,
    pub tree_id: TreeId,
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
    pub pack_key: String,
    pub pack_hash: Hash32,
    pub compressed_offset: u64,
    pub compressed_len: u32,
    pub uncompressed_len: u32,
}

pub fn tree_id(tree: &LogicalTreeManifest) -> Result<TreeId> {
    let json = serde_json::to_vec(tree).context("serializing logical tree manifest")?;
    Ok(TreeId::new(Hash32::digest(&json)))
}

pub fn layout_id(layout: &LayoutManifest) -> Result<LayoutId> {
    let json = serde_json::to_vec(layout).context("serializing layout manifest")?;
    Ok(LayoutId::new(Hash32::digest(&json)))
}

pub fn tree_key(tree_id: TreeId) -> String {
    format!("trees/{tree_id}.tree")
}

pub fn layout_key(layout_id: LayoutId) -> String {
    format!("layouts/{layout_id}.layout")
}

pub async fn load_tree<S: BlobStore>(store: &S, tree_id: TreeId) -> Result<TreeManifest> {
    store.get_json(&tree_key(tree_id)).await
}

pub async fn load_layout<S: BlobStore>(store: &S, layout_id: LayoutId) -> Result<LayoutManifest> {
    store.get_json(&layout_key(layout_id)).await
}

pub async fn write_layout<S: BlobStore>(store: &S, layout: &LayoutManifest) -> Result<LayoutId> {
    let id = layout_id(layout)?;
    store.put_json(&layout_key(id), layout).await?;
    Ok(id)
}

pub async fn write_tree<S: BlobStore>(
    store: &S,
    logical: &LogicalTreeManifest,
    locations: Vec<ChunkLocationHint>,
) -> Result<TreeId> {
    let id = tree_id(logical)?;
    let layout = LayoutManifest {
        version: 1,
        tree_id: id,
        locations,
    };
    let layout_id = write_layout(store, &layout).await?;
    let tree = TreeManifest {
        version: logical.version,
        files: logical.files.clone(),
        layout_id,
    };
    store.put_json(&tree_key(id), &tree).await?;
    Ok(id)
}

pub async fn pack_directory<S: BlobStore>(store: &S, directory: &Path) -> Result<TreeId> {
    pack_directory_with_config(store, directory, PackConfig::default()).await
}

pub async fn pack_directory_with_config<S: BlobStore>(
    store: &S,
    directory: &Path,
    config: PackConfig,
) -> Result<TreeId> {
    let config = config.validate()?;
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

    let mut jobs = Vec::new();
    for (index, path) in paths.into_iter().enumerate() {
        let rel = path
            .strip_prefix(&directory)
            .with_context(|| format!("relativizing {}", path.display()))?
            .to_string_lossy()
            .replace('\\', "/");
        let metadata = fs::metadata(&path)
            .await
            .with_context(|| format!("stat {}", path.display()))?;
        jobs.push(PackFileJob {
            index,
            path,
            rel,
            mode: metadata.permissions().mode(),
            size: metadata.len(),
            chunk_size: config.chunk_size,
        });
    }

    let file_count = jobs.len();
    let (job_tx, job_rx) = async_channel::bounded(config.pack_workers * 2);
    let (result_tx, result_rx) = async_channel::bounded(config.pack_workers * 2);
    let workers = spawn_pack_workers(config.pack_workers, job_rx, result_tx);

    let producer = tokio::spawn(async move {
        for job in jobs {
            if job_tx.send(job).await.is_err() {
                break;
            }
        }
    });

    let mut files = Vec::with_capacity(file_count);
    let mut locations = Vec::new();
    let mut pending = Vec::<(ChunkId, Vec<u8>, u32)>::new();
    let mut pending_compressed_len = 0usize;
    let mut buffered = BTreeMap::<usize, PackedFile>::new();
    let mut next_index = 0usize;

    for _ in 0..file_count {
        let packed = result_rx.recv().await.context("pack workers stopped")??;
        buffered.insert(packed.index, packed);
        while let Some(packed) = buffered.remove(&next_index) {
            append_packed_file(
                store,
                packed,
                &config,
                &mut files,
                &mut pending,
                &mut pending_compressed_len,
                &mut locations,
            )
            .await?;
            next_index += 1;
        }
    }
    producer.await.context("joining pack job producer")?;
    for worker in workers {
        worker.await.context("joining pack worker")?;
    }

    flush_pack(
        store,
        &config,
        &mut pending,
        &mut pending_compressed_len,
        &mut locations,
    )
    .await?;
    files.sort_by(|a, b| a.path.cmp(&b.path));
    locations.sort_by_key(|hint| (hint.pack_key.clone(), hint.compressed_offset, hint.chunk_id));
    let tree = LogicalTreeManifest { version: 1, files };
    write_tree(store, &tree, locations).await
}

async fn append_packed_file<S: BlobStore>(
    store: &S,
    packed: PackedFile,
    config: &PackConfig,
    files: &mut Vec<FileNode>,
    pending: &mut Vec<(ChunkId, Vec<u8>, u32)>,
    pending_compressed_len: &mut usize,
    locations: &mut Vec<ChunkLocationHint>,
) -> Result<()> {
    for chunk in packed.compressed_chunks {
        *pending_compressed_len += chunk.compressed.len();
        pending.push((chunk.chunk_id, chunk.compressed, chunk.uncompressed_len));
        if *pending_compressed_len >= config.pack_target_size {
            flush_pack(store, config, pending, pending_compressed_len, locations).await?;
        }
    }
    files.push(packed.file);
    Ok(())
}

fn spawn_pack_workers(
    worker_count: usize,
    job_rx: Receiver<PackFileJob>,
    result_tx: Sender<Result<PackedFile>>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let use_uring = io_uring_available();
    if !use_uring {
        warn!(
            target: "protostore::packer",
            "io_uring is unavailable; falling back to blocking file reads"
        );
    }
    let mut workers = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        let job_rx = job_rx.clone();
        let result_tx = result_tx.clone();
        workers.push(tokio::task::spawn_blocking(move || {
            if use_uring {
                tokio_uring::start(async move {
                    while let Ok(job) = job_rx.recv().await {
                        debug!(
                            target: "protostore::packer",
                            worker_id,
                            path = %job.path.display(),
                            "pack worker processing file with io_uring"
                        );
                        let result = pack_file_with_uring(job).await;
                        if result_tx.send(result).await.is_err() {
                            break;
                        }
                    }
                });
            } else {
                while let Ok(job) = job_rx.recv_blocking() {
                    debug!(
                        target: "protostore::packer",
                        worker_id,
                        path = %job.path.display(),
                        "pack worker processing file with blocking reads"
                    );
                    let result = pack_file_blocking(job);
                    if result_tx.send_blocking(result).is_err() {
                        break;
                    }
                }
            }
        }));
    }
    workers
}

fn io_uring_available() -> bool {
    io_uring::IoUring::new(1).is_ok()
}

async fn pack_file_with_uring(job: PackFileJob) -> Result<PackedFile> {
    let file = tokio_uring::fs::File::open(&job.path)
        .await
        .with_context(|| format!("open {}", job.path.display()))?;
    let mut offset = 0u64;
    let mut refs = Vec::new();
    let mut compressed_chunks = Vec::new();

    loop {
        let buffer = vec![0_u8; job.chunk_size];
        let (result, mut buffer) = file.read_at(buffer, offset).await;
        let n = result.with_context(|| format!("read {}", job.path.display()))?;
        if n == 0 {
            break;
        }
        buffer.truncate(n);
        let chunk_id = ChunkId::new(Hash32::digest(&buffer));
        let compressed = compress_chunk(&buffer)?;
        let uncompressed_len = u32::try_from(n).context("chunk length overflow")?;
        refs.push(FileChunkRef {
            file_offset: offset,
            uncompressed_len,
            chunk_id,
        });
        compressed_chunks.push(CompressedChunk {
            chunk_id,
            compressed,
            uncompressed_len,
        });
        offset += n as u64;
    }

    Ok(PackedFile {
        index: job.index,
        file: FileNode {
            path: job.rel,
            mode: job.mode,
            size: job.size,
            chunks: refs,
        },
        compressed_chunks,
    })
}

fn pack_file_blocking(job: PackFileJob) -> Result<PackedFile> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file =
        std::fs::File::open(&job.path).with_context(|| format!("open {}", job.path.display()))?;
    let mut offset = 0u64;
    let mut refs = Vec::new();
    let mut compressed_chunks = Vec::new();

    loop {
        let mut buffer = vec![0_u8; job.chunk_size];
        file.seek(SeekFrom::Start(offset))
            .with_context(|| format!("seek {}", job.path.display()))?;
        let n = file
            .read(&mut buffer)
            .with_context(|| format!("read {}", job.path.display()))?;
        if n == 0 {
            break;
        }
        buffer.truncate(n);
        let chunk_id = ChunkId::new(Hash32::digest(&buffer));
        let compressed = compress_chunk(&buffer)?;
        let uncompressed_len = u32::try_from(n).context("chunk length overflow")?;
        refs.push(FileChunkRef {
            file_offset: offset,
            uncompressed_len,
            chunk_id,
        });
        compressed_chunks.push(CompressedChunk {
            chunk_id,
            compressed,
            uncompressed_len,
        });
        offset += n as u64;
    }

    Ok(PackedFile {
        index: job.index,
        file: FileNode {
            path: job.rel,
            mode: job.mode,
            size: job.size,
            chunks: refs,
        },
        compressed_chunks,
    })
}

#[derive(Debug)]
struct PackFileJob {
    index: usize,
    path: PathBuf,
    rel: String,
    mode: u32,
    size: u64,
    chunk_size: usize,
}

#[derive(Debug)]
struct PackedFile {
    index: usize,
    file: FileNode,
    compressed_chunks: Vec<CompressedChunk>,
}

#[derive(Debug)]
struct CompressedChunk {
    chunk_id: ChunkId,
    compressed: Vec<u8>,
    uncompressed_len: u32,
}

async fn flush_pack<S: BlobStore>(
    store: &S,
    config: &PackConfig,
    pending: &mut Vec<(ChunkId, Vec<u8>, u32)>,
    pending_compressed_len: &mut usize,
    locations: &mut Vec<ChunkLocationHint>,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let chunks = std::mem::take(pending);
    let key = next_pack_key(config, locations);
    let (pack_id, index) =
        stream_pack_to_key(store, &key, chunks, PACK_TARGET_SIZE.min(64 * 1024 * 1024)).await?;
    let pack_hash = pack_id.0;
    for entry in index.chunks {
        locations.push(ChunkLocationHint {
            chunk_id: entry.chunk_id,
            pack_key: key.clone(),
            pack_hash,
            compressed_offset: entry.compressed_offset,
            compressed_len: entry.compressed_len,
            uncompressed_len: entry.uncompressed_len,
        });
    }
    *pending_compressed_len = 0;
    Ok(())
}

fn next_pack_key(config: &PackConfig, locations: &[ChunkLocationHint]) -> String {
    let pack_index = locations
        .iter()
        .map(|hint| hint.pack_key.as_str())
        .collect::<std::collections::HashSet<_>>()
        .len();
    format!("packs/{}/{pack_index:06}.pack", config.pack_key_prefix)
}

pub fn inspect_tree(tree_id: TreeId, tree: &TreeManifest, layout: &LayoutManifest) -> String {
    let file_count = tree.files.len();
    let chunk_count = layout.locations.len();
    let total_size: u64 = tree.files.iter().map(|file| file.size).sum();
    let layout_id = tree.layout_id;
    format!(
        "tree {tree_id}\nlayout: {layout_id}\nfiles: {file_count}\nchunks: {chunk_count}\nlogical bytes: {total_size}"
    )
}

pub async fn materialize_tree<S: BlobStore>(
    store: S,
    tree_id: TreeId,
    output_dir: &Path,
    cache: LocalCache,
) -> Result<()> {
    materialize_tree_with_config(store, tree_id, output_dir, cache, ReadConfig::default()).await
}

pub async fn materialize_tree_with_config<S: BlobStore>(
    store: S,
    tree_id: TreeId,
    output_dir: &Path,
    cache: LocalCache,
    read_config: ReadConfig,
) -> Result<()> {
    let reader = TreeReader::open_with_config(store, tree_id, cache, read_config).await?;
    reader.materialize(output_dir).await
}

pub async fn repack_tree<S: BlobStore>(
    store: &S,
    tree_id: TreeId,
    profile_id: ProfileId,
) -> Result<TreeId> {
    repack_tree_with_config(store, tree_id, profile_id, PackConfig::default()).await
}

pub async fn repack_tree_with_config<S: BlobStore>(
    store: &S,
    tree_id: TreeId,
    profile_id: ProfileId,
    config: PackConfig,
) -> Result<TreeId> {
    let config = config.validate()?;
    let tree = load_tree(store, tree_id).await?;
    let layout = load_layout(store, tree.layout_id).await?;
    ensure!(
        layout.tree_id == tree_id,
        "layout {} belongs to tree {}, not {}",
        tree.layout_id,
        layout.tree_id,
        tree_id
    );
    let profile = load_profile(store, profile_id).await?;
    let read_times: HashMap<ChunkId, u128> = profile
        .events
        .into_iter()
        .map(|event| (event.chunk_id, event.first_read_unix_ns))
        .collect();
    let location_by_chunk: HashMap<ChunkId, ChunkLocationHint> = layout
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
        let decompressed = read_chunk_from_pack_key(store, &hint.pack_key, &entry).await?;
        ensure!(
            ChunkId::new(Hash32::digest(&decompressed)) == chunk_id,
            "chunk hash mismatch while repacking {chunk_id}"
        );
        let compressed = compress_chunk(&decompressed)?;
        pending_len += compressed.len();
        pending.push((chunk_id, compressed, hint.uncompressed_len));
        if pending_len >= config.pack_target_size {
            flush_pack(
                store,
                &config,
                &mut pending,
                &mut pending_len,
                &mut new_locations,
            )
            .await?;
        }
    }
    flush_pack(
        store,
        &config,
        &mut pending,
        &mut pending_len,
        &mut new_locations,
    )
    .await?;
    new_locations
        .sort_by_key(|hint| (hint.pack_key.clone(), hint.compressed_offset, hint.chunk_id));
    let new_layout = LayoutManifest {
        version: 1,
        tree_id,
        locations: new_locations,
    };
    let layout_id = write_layout(store, &new_layout).await?;
    let new_tree = TreeManifest {
        version: tree.version,
        files: tree.files,
        layout_id,
    };
    store.put_json(&tree_key(tree_id), &new_tree).await?;
    Ok(tree_id)
}

pub fn file_map(tree: &TreeManifest) -> BTreeMap<String, FileNode> {
    tree.files
        .iter()
        .map(|file| (file.path.clone(), file.clone()))
        .collect()
}

pub fn location_map(layout: &LayoutManifest) -> HashMap<ChunkId, ChunkLocationHint> {
    layout
        .locations
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
