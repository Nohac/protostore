use crate::{
    cache::LocalCache,
    hash::{ChunkId, Hash32, LayoutId, ProfileId, TreeId},
    pack::{
        ChunkEntry, Compression, StreamingPackWriter, compress_chunk, read_chunk_from_pack_key,
    },
    profile::load_profile,
    reader::{ReadConfig, TreeReader},
    store::BlobStore,
};
use anyhow::{Context, Result, anyhow, bail, ensure};
use async_channel::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::Permissions,
    os::unix::fs::FileExt,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};
use tokio::fs;
use tracing::{debug, warn};
use walkdir::WalkDir;

pub const DEFAULT_CHUNK_SIZE: usize = 16 * 1024 * 1024;
const STREAM_UPLOAD_PART_SIZE: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackConfig {
    pub chunk_size: usize,
    pub pack_workers: usize,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackedTree {
    pub key: String,
    pub tree_id: TreeId,
}

impl Default for PackConfig {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
            pack_workers: default_pack_workers(),
            key: generated_pack_key(),
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
            self.pack_workers > 0,
            "pack workers must be greater than zero"
        );
        ensure!(
            valid_object_key(&self.key),
            "pack key must be relative and must not contain empty path segments or '..'"
        );
        Ok(self)
    }
}

fn generated_pack_key() -> String {
    uuid::Uuid::now_v7().to_string()
}

fn valid_object_key(value: &str) -> bool {
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
    pub tree_id: TreeId,
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
    #[serde(default)]
    pub chunk_offset: u32,
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

pub fn tree_key(key: &str) -> String {
    format!("trees/{key}.tree")
}

pub fn layout_key(key: &str) -> String {
    format!("layouts/{key}.layout")
}

pub async fn load_tree<S: BlobStore>(store: &S, key: &str) -> Result<TreeManifest> {
    ensure!(valid_object_key(key), "invalid tree key {key}");
    store.get_json(&tree_key(key)).await
}

pub async fn load_layout<S: BlobStore>(store: &S, key: &str) -> Result<LayoutManifest> {
    ensure!(valid_object_key(key), "invalid layout key {key}");
    store.get_json(&layout_key(key)).await
}

pub async fn write_layout<S: BlobStore>(
    store: &S,
    key: &str,
    layout: &LayoutManifest,
) -> Result<LayoutId> {
    ensure!(valid_object_key(key), "invalid layout key {key}");
    let id = layout_id(layout)?;
    store.put_json(&layout_key(key), layout).await?;
    Ok(id)
}

pub async fn write_tree<S: BlobStore>(
    store: &S,
    key: &str,
    logical: &LogicalTreeManifest,
    locations: Vec<ChunkLocationHint>,
) -> Result<TreeId> {
    ensure!(valid_object_key(key), "invalid tree key {key}");
    let id = tree_id(logical)?;
    let layout = LayoutManifest {
        version: 1,
        tree_id: id,
        locations,
    };
    let layout_id = write_layout(store, key, &layout).await?;
    let tree = TreeManifest {
        version: logical.version,
        tree_id: id,
        files: logical.files.clone(),
        layout_id,
    };
    store.put_json(&tree_key(key), &tree).await?;
    Ok(id)
}

pub async fn pack_directory<S: BlobStore>(store: &S, directory: &Path) -> Result<PackedTree> {
    pack_directory_with_config(store, directory, PackConfig::default()).await
}

pub async fn pack_directory_with_config<S: BlobStore>(
    store: &S,
    directory: &Path,
    config: PackConfig,
) -> Result<PackedTree> {
    let config = config.validate()?;
    let key = config.key.clone();
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
        files.push(FileNode {
            path: rel.clone(),
            mode: metadata.permissions().mode(),
            size: metadata.len(),
            chunks: Vec::new(),
        });
        jobs.push(FileJob {
            index,
            path,
            size: metadata.len(),
        });
    }

    let (file_tx, file_rx) = async_channel::bounded(config.pack_workers * 16);
    let (batch_tx, batch_rx) = async_channel::bounded(config.pack_workers * 4);
    let (ready_tx, ready_rx) = async_channel::bounded(config.pack_workers * 4);
    let workers = spawn_pack_workers(
        config.pack_workers,
        batch_rx,
        ready_tx.clone(),
        config.chunk_size,
    );
    drop(ready_tx);

    let producer = tokio::spawn(async move {
        for job in jobs {
            if file_tx.send(job).await.is_err() {
                break;
            }
        }
    });
    let batcher_chunk_size = config.chunk_size;
    let batcher =
        tokio::spawn(async move { run_batcher(file_rx, batch_tx, batcher_chunk_size).await });

    let mut locations = Vec::new();
    let mut active_pack = None;

    while let Ok(ready) = ready_rx.recv().await {
        append_ready_chunk(store, ready?, &config, &mut files, &mut active_pack).await?;
    }
    producer.await.context("joining pack job producer")?;
    batcher.await.context("joining pack batcher")??;
    for worker in workers {
        worker.await.context("joining pack worker")?;
    }

    finish_active_pack(&mut active_pack, &mut locations).await?;
    for file in &mut files {
        file.chunks.sort_by_key(|chunk| chunk.file_offset);
    }
    files.sort_by(|a, b| a.path.cmp(&b.path));
    locations.sort_by_key(|hint| (hint.pack_key.clone(), hint.compressed_offset, hint.chunk_id));
    let tree = LogicalTreeManifest { version: 1, files };
    let tree_id = write_tree(store, &key, &tree, locations).await?;
    Ok(PackedTree { key, tree_id })
}

async fn append_ready_chunk<S: BlobStore>(
    store: &S,
    ready: ReadyChunk,
    config: &PackConfig,
    files: &mut [FileNode],
    active_pack: &mut Option<ActivePack>,
) -> Result<()> {
    let chunk_id = ready.chunk_id;
    for chunk_ref in ready.refs {
        let file = files
            .get_mut(chunk_ref.file_index)
            .with_context(|| format!("missing file index {}", chunk_ref.file_index))?;
        file.chunks.push(FileChunkRef {
            file_offset: chunk_ref.file_offset,
            chunk_offset: chunk_ref.chunk_offset,
            uncompressed_len: chunk_ref.uncompressed_len,
            chunk_id,
        });
    }
    append_compressed_chunk_to_pack(
        store,
        config,
        active_pack,
        chunk_id,
        ready.compressed,
        ready.uncompressed_len,
    )
    .await
}

fn spawn_pack_workers(
    worker_count: usize,
    batch_rx: Receiver<BatchJob>,
    ready_tx: Sender<Result<ReadyChunk>>,
    chunk_size: usize,
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
        let batch_rx = batch_rx.clone();
        let ready_tx = ready_tx.clone();
        workers.push(tokio::task::spawn_blocking(move || {
            if use_uring {
                tokio_uring::start(async move {
                    while let Ok(batch) = batch_rx.recv().await {
                        debug!(
                            target: "protostore::packer",
                            worker_id,
                            file_count = batch.files.len(),
                            "pack worker processing batch with io_uring"
                        );
                        let result = process_batch_with_uring(batch, chunk_size, &ready_tx).await;
                        if let Err(error) = result {
                            let _ = ready_tx.send(Err(error)).await;
                            break;
                        }
                    }
                });
            } else {
                while let Ok(batch) = batch_rx.recv_blocking() {
                    debug!(
                        target: "protostore::packer",
                        worker_id,
                        file_count = batch.files.len(),
                        "pack worker processing batch with blocking reads"
                    );
                    let result = process_batch_blocking(batch, chunk_size, &ready_tx);
                    if let Err(error) = result {
                        let _ = ready_tx.send_blocking(Err(error));
                        break;
                    }
                }
            }
        }));
    }
    workers
}

async fn run_batcher(
    file_rx: Receiver<FileJob>,
    batch_tx: Sender<BatchJob>,
    chunk_size: usize,
) -> Result<()> {
    let mut files = Vec::new();
    let mut bytes = 0usize;

    while let Ok(file) = file_rx.recv().await {
        if file.size == 0 {
            continue;
        }
        let file_size = usize::try_from(file.size).unwrap_or(usize::MAX);
        if file_size > chunk_size {
            flush_batch(&batch_tx, &mut files, &mut bytes).await?;
            batch_tx
                .send(BatchJob { files: vec![file] })
                .await
                .context("sending large file batch")?;
            continue;
        }
        if bytes + file_size > chunk_size {
            flush_batch(&batch_tx, &mut files, &mut bytes).await?;
        }
        bytes += file_size;
        files.push(file);
        if bytes >= chunk_size {
            flush_batch(&batch_tx, &mut files, &mut bytes).await?;
        }
    }

    flush_batch(&batch_tx, &mut files, &mut bytes).await
}

async fn flush_batch(
    batch_tx: &Sender<BatchJob>,
    files: &mut Vec<FileJob>,
    bytes: &mut usize,
) -> Result<()> {
    if files.is_empty() {
        return Ok(());
    }
    let batch = BatchJob {
        files: std::mem::take(files),
    };
    *bytes = 0;
    batch_tx.send(batch).await.context("sending file batch")
}

fn io_uring_available() -> bool {
    io_uring::IoUring::new(1).is_ok()
}

async fn process_batch_with_uring(
    batch: BatchJob,
    chunk_size: usize,
    ready_tx: &Sender<Result<ReadyChunk>>,
) -> Result<()> {
    if batch.files.len() > 1 {
        let ready = bundle_small_files_with_uring(batch.files).await?;
        send_ready_async(ready_tx, ready).await?;
        return Ok(());
    }

    if let Some(file) = batch.files.into_iter().next() {
        split_file_with_uring(file, chunk_size, ready_tx).await?;
    }
    Ok(())
}

async fn bundle_small_files_with_uring(files: Vec<FileJob>) -> Result<ReadyChunk> {
    let total_len = files.iter().try_fold(0usize, |acc, file| {
        Ok::<_, anyhow::Error>(acc + usize::try_from(file.size)?)
    })?;
    let mut payload = Vec::with_capacity(total_len);
    let mut refs = Vec::with_capacity(files.len());
    for file in files {
        let chunk_offset = u32::try_from(payload.len()).context("bundle chunk offset overflow")?;
        let bytes = read_full_file_with_uring(&file).await?;
        let uncompressed_len = u32::try_from(bytes.len()).context("file chunk length overflow")?;
        refs.push(ReadyChunkRef {
            file_index: file.index,
            file_offset: 0,
            chunk_offset,
            uncompressed_len,
        });
        payload.extend_from_slice(&bytes);
    }
    ready_chunk(refs, payload)
}

async fn split_file_with_uring(
    file: FileJob,
    chunk_size: usize,
    ready_tx: &Sender<Result<ReadyChunk>>,
) -> Result<()> {
    let source = tokio_uring::fs::File::open(&file.path)
        .await
        .with_context(|| format!("open {}", file.path.display()))?;
    let mut offset = 0u64;
    while offset < file.size {
        let len = chunk_size.min(usize::try_from(file.size - offset).unwrap_or(usize::MAX));
        let payload = read_uring_range(&source, &file.path, offset, len).await?;
        let uncompressed_len =
            u32::try_from(payload.len()).context("file chunk length overflow")?;
        let ready = ready_chunk(
            vec![ReadyChunkRef {
                file_index: file.index,
                file_offset: offset,
                chunk_offset: 0,
                uncompressed_len,
            }],
            payload,
        )?;
        send_ready_async(ready_tx, ready).await?;
        offset += u64::from(uncompressed_len);
    }
    Ok(())
}

async fn read_full_file_with_uring(file: &FileJob) -> Result<Vec<u8>> {
    let source = tokio_uring::fs::File::open(&file.path)
        .await
        .with_context(|| format!("open {}", file.path.display()))?;
    read_uring_range(&source, &file.path, 0, usize::try_from(file.size)?).await
}

async fn read_uring_range(
    file: &tokio_uring::fs::File,
    path: &Path,
    offset: u64,
    len: usize,
) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(len);
    let mut read = 0usize;
    while read < len {
        let buffer = vec![0_u8; len - read];
        let (result, buffer) = file.read_at(buffer, offset + read as u64).await;
        let n = result.with_context(|| format!("read {}", path.display()))?;
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buffer[..n]);
        read += n;
    }
    ensure!(
        read == len,
        "short read from {}: expected {len}, got {read}",
        path.display()
    );
    Ok(out)
}

async fn send_ready_async(ready_tx: &Sender<Result<ReadyChunk>>, ready: ReadyChunk) -> Result<()> {
    ready_tx
        .send(Ok(ready))
        .await
        .map_err(|_| anyhow!("pack writer stopped"))
}

fn process_batch_blocking(
    batch: BatchJob,
    chunk_size: usize,
    ready_tx: &Sender<Result<ReadyChunk>>,
) -> Result<()> {
    if batch.files.len() > 1 {
        let ready = bundle_small_files_blocking(batch.files)?;
        send_ready_blocking(ready_tx, ready)?;
        return Ok(());
    }

    if let Some(file) = batch.files.into_iter().next() {
        split_file_blocking(file, chunk_size, ready_tx)?;
    }
    Ok(())
}

fn bundle_small_files_blocking(files: Vec<FileJob>) -> Result<ReadyChunk> {
    let total_len = files.iter().try_fold(0usize, |acc, file| {
        Ok::<_, anyhow::Error>(acc + usize::try_from(file.size)?)
    })?;
    let mut payload = Vec::with_capacity(total_len);
    let mut refs = Vec::with_capacity(files.len());
    for file in files {
        let chunk_offset = u32::try_from(payload.len()).context("bundle chunk offset overflow")?;
        let bytes = read_full_file_blocking(&file)?;
        let uncompressed_len = u32::try_from(bytes.len()).context("file chunk length overflow")?;
        refs.push(ReadyChunkRef {
            file_index: file.index,
            file_offset: 0,
            chunk_offset,
            uncompressed_len,
        });
        payload.extend_from_slice(&bytes);
    }
    ready_chunk(refs, payload)
}

fn split_file_blocking(
    file: FileJob,
    chunk_size: usize,
    ready_tx: &Sender<Result<ReadyChunk>>,
) -> Result<()> {
    let source =
        std::fs::File::open(&file.path).with_context(|| format!("open {}", file.path.display()))?;
    let mut offset = 0u64;
    while offset < file.size {
        let len = chunk_size.min(usize::try_from(file.size - offset).unwrap_or(usize::MAX));
        let payload = read_blocking_range(&source, &file.path, offset, len)?;
        let uncompressed_len =
            u32::try_from(payload.len()).context("file chunk length overflow")?;
        let ready = ready_chunk(
            vec![ReadyChunkRef {
                file_index: file.index,
                file_offset: offset,
                chunk_offset: 0,
                uncompressed_len,
            }],
            payload,
        )?;
        send_ready_blocking(ready_tx, ready)?;
        offset += u64::from(uncompressed_len);
    }
    Ok(())
}

fn read_full_file_blocking(file: &FileJob) -> Result<Vec<u8>> {
    let source =
        std::fs::File::open(&file.path).with_context(|| format!("open {}", file.path.display()))?;
    read_blocking_range(&source, &file.path, 0, usize::try_from(file.size)?)
}

fn read_blocking_range(
    file: &std::fs::File,
    path: &Path,
    offset: u64,
    len: usize,
) -> Result<Vec<u8>> {
    let mut out = vec![0_u8; len];
    let mut read = 0usize;
    while read < len {
        let n = file
            .read_at(&mut out[read..], offset + read as u64)
            .with_context(|| format!("read {}", path.display()))?;
        if n == 0 {
            break;
        }
        read += n;
    }
    ensure!(
        read == len,
        "short read from {}: expected {len}, got {read}",
        path.display()
    );
    Ok(out)
}

fn send_ready_blocking(ready_tx: &Sender<Result<ReadyChunk>>, ready: ReadyChunk) -> Result<()> {
    ready_tx
        .send_blocking(Ok(ready))
        .map_err(|_| anyhow!("pack writer stopped"))
}

fn ready_chunk(refs: Vec<ReadyChunkRef>, payload: Vec<u8>) -> Result<ReadyChunk> {
    let chunk_id = ChunkId::new(Hash32::digest(&payload));
    let uncompressed_len = u32::try_from(payload.len()).context("chunk length overflow")?;
    let compressed = compress_chunk(&payload)?;
    Ok(ReadyChunk {
        refs,
        chunk_id,
        compressed,
        uncompressed_len,
    })
}

#[derive(Debug)]
struct FileJob {
    index: usize,
    path: PathBuf,
    size: u64,
}

#[derive(Debug)]
struct BatchJob {
    files: Vec<FileJob>,
}

#[derive(Debug)]
struct ReadyChunk {
    refs: Vec<ReadyChunkRef>,
    chunk_id: ChunkId,
    compressed: Vec<u8>,
    uncompressed_len: u32,
}

#[derive(Debug)]
struct ReadyChunkRef {
    file_index: usize,
    file_offset: u64,
    chunk_offset: u32,
    uncompressed_len: u32,
}

struct ActivePack {
    key: String,
    writer: StreamingPackWriter,
}

async fn start_active_pack<S: BlobStore>(store: &S, config: &PackConfig) -> Result<ActivePack> {
    let key = pack_key(config);
    let writer = StreamingPackWriter::start(store, &key, STREAM_UPLOAD_PART_SIZE).await?;
    Ok(ActivePack { key, writer })
}

async fn finish_active_pack(
    active_pack: &mut Option<ActivePack>,
    locations: &mut Vec<ChunkLocationHint>,
) -> Result<()> {
    let Some(pack) = active_pack.take() else {
        return Ok(());
    };
    let key = pack.key;
    let (pack_id, index) = pack.writer.finish().await?;
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
    Ok(())
}

async fn append_compressed_chunk_to_pack<S: BlobStore>(
    store: &S,
    config: &PackConfig,
    active_pack: &mut Option<ActivePack>,
    chunk_id: ChunkId,
    compressed: Vec<u8>,
    uncompressed_len: u32,
) -> Result<()> {
    if active_pack.is_none() {
        *active_pack = Some(start_active_pack(store, config).await?);
    }
    let pack = active_pack
        .as_mut()
        .context("active pack missing after start")?;
    pack.writer
        .append_chunk(chunk_id, compressed, uncompressed_len)
        .await
}

fn pack_key(config: &PackConfig) -> String {
    format!("packs/{}.pack", config.key)
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
    key: &str,
    output_dir: &Path,
    cache: LocalCache,
) -> Result<()> {
    materialize_tree_with_config(store, key, output_dir, cache, ReadConfig::default()).await
}

pub async fn materialize_tree_with_config<S: BlobStore>(
    store: S,
    key: &str,
    output_dir: &Path,
    cache: LocalCache,
    read_config: ReadConfig,
) -> Result<()> {
    let reader = TreeReader::open_with_config(store, key, cache, read_config).await?;
    reader.materialize(output_dir).await
}

pub async fn repack_tree<S: BlobStore>(
    store: &S,
    key: &str,
    profile_id: ProfileId,
) -> Result<PackedTree> {
    repack_tree_with_config(store, key, profile_id, PackConfig::default()).await
}

pub async fn repack_tree_with_config<S: BlobStore>(
    store: &S,
    key: &str,
    profile_id: ProfileId,
    config: PackConfig,
) -> Result<PackedTree> {
    let config = config.validate()?;
    let output_key = config.key.clone();
    let tree = load_tree(store, key).await?;
    let layout = load_layout(store, key).await?;
    let tree_id = tree.tree_id;
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
    let mut active_pack = None;
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
        append_compressed_chunk_to_pack(
            store,
            &config,
            &mut active_pack,
            chunk_id,
            compressed,
            hint.uncompressed_len,
        )
        .await?;
    }
    finish_active_pack(&mut active_pack, &mut new_locations).await?;
    new_locations
        .sort_by_key(|hint| (hint.pack_key.clone(), hint.compressed_offset, hint.chunk_id));
    let new_layout = LayoutManifest {
        version: 1,
        tree_id,
        locations: new_locations,
    };
    let layout_id = write_layout(store, &output_key, &new_layout).await?;
    let new_tree = TreeManifest {
        version: tree.version,
        tree_id,
        files: tree.files,
        layout_id,
    };
    store.put_json(&tree_key(&output_key), &new_tree).await?;
    Ok(PackedTree {
        key: output_key,
        tree_id,
    })
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
