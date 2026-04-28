use crate::{
    cache::LocalCache,
    hash::TreeId,
    pack::decompress_chunk,
    profile::ProfileRecorder,
    store::BlobStore,
    tree::{FileNode, TreeManifest, load_tree, location_map, safe_output_path, write_file_mode},
};
use anyhow::{Context, Result, bail, ensure};
use bytes::{Bytes, BytesMut};
use std::{
    collections::{HashMap, HashSet},
    ops::Range,
    path::Path,
    sync::Arc,
};
use tokio::{
    fs,
    io::AsyncWriteExt,
    sync::{Mutex, Semaphore, mpsc, oneshot},
};
use tracing::{debug, info, warn};

#[derive(Clone)]
pub struct TreeReader<S> {
    _store: S,
    tree_id: TreeId,
    tree: TreeManifest,
    files: HashMap<String, FileNode>,
    locations: HashMap<crate::ChunkId, crate::ChunkLocationHint>,
    _pack_data_end: Arc<HashMap<String, u64>>,
    cache: LocalCache,
    recorder: Option<ProfileRecorder>,
    read_config: ReadConfig,
    chunks: ChunkCoordinator,
    prefetching: Arc<Mutex<HashSet<crate::ChunkId>>>,
    prefetch_permits: Arc<Semaphore>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadConfig {
    pub min_remote_read: usize,
    pub target_coalesce: usize,
    pub read_ahead_chunks: usize,
    pub read_ahead_bytes: usize,
    pub read_ahead_concurrency: usize,
}

impl Default for ReadConfig {
    fn default() -> Self {
        Self {
            min_remote_read: 16 * 1024 * 1024,
            target_coalesce: 64 * 1024 * 1024,
            read_ahead_chunks: 4,
            read_ahead_bytes: 64 * 1024 * 1024,
            read_ahead_concurrency: 2,
        }
    }
}

impl ReadConfig {
    pub fn validate(self) -> Result<Self> {
        ensure!(
            self.min_remote_read > 0,
            "minimum remote read must be greater than zero"
        );
        ensure!(
            self.target_coalesce >= self.min_remote_read,
            "target coalesce size must be at least min remote read size"
        );
        ensure!(
            self.read_ahead_concurrency > 0
                || self.read_ahead_chunks == 0
                || self.read_ahead_bytes == 0,
            "read ahead concurrency must be greater than zero when read ahead is enabled"
        );
        Ok(self)
    }
}

impl<S: BlobStore> TreeReader<S> {
    pub async fn open(store: S, key: impl Into<String>, cache: LocalCache) -> Result<Self> {
        Self::open_with_config(store, key, cache, ReadConfig::default()).await
    }

    pub async fn open_with_config(
        store: S,
        key: impl Into<String>,
        cache: LocalCache,
        read_config: ReadConfig,
    ) -> Result<Self> {
        let key = key.into();
        let tree = load_tree(&store, &key).await?;
        Self::from_manifest_with_config(store, key, tree, cache, None, read_config)
    }

    pub fn from_manifest(
        store: S,
        _key: impl Into<String>,
        tree: TreeManifest,
        cache: LocalCache,
        recorder: Option<ProfileRecorder>,
    ) -> Result<Self> {
        Self::from_manifest_with_config(store, _key, tree, cache, recorder, ReadConfig::default())
    }

    pub fn from_manifest_with_config(
        store: S,
        _key: impl Into<String>,
        tree: TreeManifest,
        cache: LocalCache,
        recorder: Option<ProfileRecorder>,
        read_config: ReadConfig,
    ) -> Result<Self> {
        let tree_id = tree.tree_id;
        let read_config = read_config.validate()?;
        let files = tree
            .files
            .iter()
            .map(|file| (file.path.clone(), file.clone()))
            .collect();
        let locations = location_map(&tree);
        let mut pack_data_end = HashMap::<String, u64>::new();
        for hint in &tree.locations {
            let end = hint.compressed_offset + u64::from(hint.compressed_len);
            pack_data_end
                .entry(hint.pack_key.clone())
                .and_modify(|current| *current = (*current).max(end))
                .or_insert(end);
        }
        let pack_data_end = Arc::new(pack_data_end);
        let chunks = ChunkCoordinator::spawn(
            store.clone(),
            cache.clone(),
            read_config,
            pack_data_end.clone(),
            tree_id,
        );
        Ok(Self {
            _store: store,
            tree_id,
            tree,
            files,
            locations,
            _pack_data_end: pack_data_end,
            cache,
            recorder,
            read_config,
            chunks,
            prefetching: Arc::new(Mutex::new(HashSet::new())),
            prefetch_permits: Arc::new(Semaphore::new(read_config.read_ahead_concurrency.max(1))),
        })
    }

    pub fn tree(&self) -> &TreeManifest {
        &self.tree
    }

    pub fn recorder(&self) -> Option<ProfileRecorder> {
        self.recorder.clone()
    }

    pub fn with_recorder(mut self, recorder: ProfileRecorder) -> Self {
        self.recorder = Some(recorder);
        self
    }

    pub fn with_read_config(mut self, read_config: ReadConfig) -> Result<Self> {
        self.read_config = read_config.validate()?;
        Ok(self)
    }

    pub async fn read_at(&self, path: &str, offset: u64, len: usize) -> Result<Bytes> {
        let file = self
            .files
            .get(path)
            .with_context(|| format!("file not found in tree: {path}"))?;
        if offset >= file.size || len == 0 {
            debug!(
                target: "protostore::reader",
                tree_id = %self.tree_id,
                path,
                offset,
                len,
                file_size = file.size,
                "read_at empty"
            );
            return Ok(Bytes::new());
        }
        let end = (offset + len as u64).min(file.size);
        info!(
            target: "protostore::reader",
            tree_id = %self.tree_id,
            path,
            offset,
            requested_len = len,
            end,
            file_size = file.size,
            "read_at"
        );
        let mut out = BytesMut::with_capacity((end - offset) as usize);
        let mut selected = Vec::new();
        for chunk in &file.chunks {
            let chunk_start = chunk.file_offset;
            let chunk_end = chunk.file_offset + u64::from(chunk.uncompressed_len);
            if chunk_end <= offset || chunk_start >= end {
                continue;
            }
            let take_start = offset.saturating_sub(chunk_start) as usize;
            let take_end = (end.min(chunk_end) - chunk_start) as usize;
            debug!(
                target: "protostore::reader",
                tree_id = %self.tree_id,
                path,
                chunk_id = %chunk.chunk_id,
                chunk_file_offset = chunk.file_offset,
                chunk_payload_offset = chunk.chunk_offset,
                chunk_uncompressed_len = chunk.uncompressed_len,
                take_start,
                take_end,
                "read_at selected chunk"
            );
            selected.push(SelectedChunk {
                chunk_id: chunk.chunk_id,
                chunk_offset: chunk.chunk_offset as usize,
                take_start,
                take_end,
            });
        }
        let chunks = self.read_chunks(&selected).await?;
        self.schedule_read_ahead(file, end, &selected).await?;
        for selected in selected {
            let slice = chunks
                .get(&selected.chunk_id)
                .with_context(|| format!("missing decoded chunk range {}", selected.chunk_id))?;
            out.extend_from_slice(slice);
        }
        Ok(out.freeze())
    }

    async fn read_chunks(
        &self,
        selected: &[SelectedChunk],
    ) -> Result<HashMap<crate::ChunkId, Bytes>> {
        let mut out = HashMap::new();
        let mut requests = Vec::new();
        for selected in selected {
            let chunk_id = selected.chunk_id;
            let start = selected.chunk_offset + selected.take_start;
            let end = selected.chunk_offset + selected.take_end;
            if let Some(bytes) = self.cache.get_chunk(chunk_id).await? {
                debug!(
                    target: "protostore::reader",
                    tree_id = %self.tree_id,
                    chunk_id = %chunk_id,
                    uncompressed_len = bytes.len(),
                    "chunk cache hit"
                );
                if let Some(recorder) = &self.recorder {
                    recorder.record(self.tree_id, chunk_id);
                }
                ensure!(
                    end <= bytes.len(),
                    "cached chunk range {start}..{end} is outside {chunk_id}"
                );
                let slice = bytes.slice(start..end);
                out.insert(chunk_id, slice);
            } else {
                debug!(
                    target: "protostore::reader",
                    tree_id = %self.tree_id,
                    chunk_id = %chunk_id,
                    "chunk cache miss"
                );
                let hint = self
                    .locations
                    .get(&chunk_id)
                    .with_context(|| format!("missing location for chunk {chunk_id}"))?;
                requests.push(ChunkSliceRequest {
                    hint: hint.clone(),
                    range: start..end,
                });
            }
        }

        let groups = self.coalesce_misses(requests);
        for group in groups {
            let chunks = self.chunks.get(group).await?;
            for (chunk_id, bytes) in chunks {
                if let Some(recorder) = &self.recorder {
                    recorder.record(self.tree_id, chunk_id);
                }
                out.insert(chunk_id, bytes);
            }
        }

        Ok(out)
    }

    fn coalesce_misses(&self, mut misses: Vec<ChunkSliceRequest>) -> Vec<Vec<ChunkSliceRequest>> {
        misses.sort_by_key(|request| {
            (
                request.hint.pack_key.clone(),
                request.hint.compressed_offset,
                request.hint.chunk_id,
            )
        });
        let mut groups: Vec<Vec<ChunkSliceRequest>> = Vec::new();
        for request in misses {
            let Some(current) = groups.last_mut() else {
                groups.push(vec![request]);
                continue;
            };
            let first = &current.first().unwrap().hint;
            let last = &current.last().unwrap().hint;
            let hint = &request.hint;
            let current_start = first.compressed_offset;
            let current_end = last.compressed_offset + u64::from(last.compressed_len);
            let next_end = hint.compressed_offset + u64::from(hint.compressed_len);
            let coalesced_len = next_end.saturating_sub(current_start);
            if hint.pack_key == first.pack_key
                && hint.compressed_offset >= current_end
                && coalesced_len <= self.read_config.target_coalesce as u64
            {
                current.push(request);
            } else {
                groups.push(vec![request]);
            }
        }
        groups
    }

    async fn schedule_read_ahead(
        &self,
        file: &FileNode,
        read_end: u64,
        selected: &[SelectedChunk],
    ) -> Result<()> {
        if self.read_config.read_ahead_chunks == 0 || self.read_config.read_ahead_bytes == 0 {
            return Ok(());
        }

        let selected: HashSet<_> = selected.iter().map(|chunk| chunk.chunk_id).collect();
        let mut hints = Vec::new();
        let mut compressed_bytes = 0usize;

        for chunk in &file.chunks {
            let chunk_end = chunk.file_offset + u64::from(chunk.uncompressed_len);
            if chunk_end <= read_end || selected.contains(&chunk.chunk_id) {
                continue;
            }
            if self.cache.contains_chunk(chunk.chunk_id).await? {
                continue;
            }

            let hint = self.locations.get(&chunk.chunk_id).with_context(|| {
                format!("missing location for read-ahead chunk {}", chunk.chunk_id)
            })?;

            {
                let mut prefetching = self.prefetching.lock().await;
                if !prefetching.insert(chunk.chunk_id) {
                    continue;
                }
            }

            compressed_bytes = compressed_bytes.saturating_add(hint.compressed_len as usize);
            hints.push(hint.clone());
            if hints.len() >= self.read_config.read_ahead_chunks
                || compressed_bytes >= self.read_config.read_ahead_bytes
            {
                break;
            }
        }

        if hints.is_empty() {
            return Ok(());
        }

        let permit = match self.prefetch_permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                let mut prefetching = self.prefetching.lock().await;
                for hint in hints {
                    prefetching.remove(&hint.chunk_id);
                }
                debug!(
                    target: "protostore::reader",
                    tree_id = %self.tree_id,
                    path = %file.path,
                    "skip read-ahead because all prefetch slots are busy"
                );
                return Ok(());
            }
        };

        let reader = self.clone();
        let path = file.path.clone();
        tokio::spawn(async move {
            let chunk_ids: Vec<_> = hints.iter().map(|hint| hint.chunk_id).collect();
            info!(
                target: "protostore::reader",
                tree_id = %reader.tree_id,
                path,
                chunk_count = hints.len(),
                "start read-ahead"
            );
            if let Err(error) = reader.prefetch_hints(hints).await {
                warn!(
                    target: "protostore::reader",
                    tree_id = %reader.tree_id,
                    error = ?error,
                    "read-ahead failed"
                );
            }
            let mut prefetching = reader.prefetching.lock().await;
            for chunk_id in chunk_ids {
                prefetching.remove(&chunk_id);
            }
            drop(permit);
        });

        Ok(())
    }

    async fn prefetch_hints(&self, hints: Vec<crate::ChunkLocationHint>) -> Result<()> {
        let mut misses = Vec::new();
        for hint in hints {
            if self.cache.contains_chunk(hint.chunk_id).await? {
                continue;
            }
            let uncompressed_len = hint.uncompressed_len as usize;
            misses.push(ChunkSliceRequest {
                hint,
                range: 0..uncompressed_len,
            });
        }

        for group in self.coalesce_misses(misses) {
            self.chunks.get(group).await?;
        }
        Ok(())
    }

    pub async fn materialize(&self, output_dir: &Path) -> Result<()> {
        fs::create_dir_all(output_dir)
            .await
            .with_context(|| format!("creating {}", output_dir.display()))?;
        for file in &self.tree.files {
            let path = safe_output_path(output_dir, &file.path)?;
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)
                    .await
                    .with_context(|| format!("creating {}", parent.display()))?;
            }
            let mut out = fs::File::create(&path)
                .await
                .with_context(|| format!("creating {}", path.display()))?;
            let bytes = self
                .read_at(
                    &file.path,
                    0,
                    usize::try_from(file.size).unwrap_or(usize::MAX),
                )
                .await?;
            out.write_all(&bytes)
                .await
                .with_context(|| format!("writing {}", path.display()))?;
            out.flush()
                .await
                .with_context(|| format!("flushing {}", path.display()))?;
            write_file_mode(&path, file.mode).await?;
            let actual = fs::metadata(&path)
                .await
                .with_context(|| format!("stat {}", path.display()))?
                .len();
            if actual != file.size {
                bail!("materialized size mismatch for {}", file.path);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct SelectedChunk {
    chunk_id: crate::ChunkId,
    chunk_offset: usize,
    take_start: usize,
    take_end: usize,
}

#[derive(Debug, Clone)]
struct ChunkSliceRequest {
    hint: crate::ChunkLocationHint,
    range: Range<usize>,
}

#[derive(Clone)]
struct ChunkCoordinator {
    requests: mpsc::Sender<ChunkBatchRequest>,
}

impl ChunkCoordinator {
    fn spawn<S: BlobStore>(
        store: S,
        cache: LocalCache,
        read_config: ReadConfig,
        pack_data_end: Arc<HashMap<String, u64>>,
        tree_id: TreeId,
    ) -> Self {
        let (requests_tx, requests_rx) = mpsc::channel(256);
        let (completed_tx, completed_rx) = mpsc::channel(256);
        let fetch_permits = Arc::new(Semaphore::new(
            read_config.read_ahead_concurrency.max(1) + 4,
        ));
        tokio::spawn(run_chunk_coordinator(
            store,
            cache,
            read_config,
            pack_data_end,
            tree_id,
            fetch_permits,
            requests_rx,
            completed_tx,
            completed_rx,
        ));
        Self {
            requests: requests_tx,
        }
    }

    async fn get(&self, chunks: Vec<ChunkSliceRequest>) -> Result<HashMap<crate::ChunkId, Bytes>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.requests
            .send(ChunkBatchRequest {
                chunks,
                response: response_tx,
            })
            .await
            .context("sending chunk request to coordinator")?;
        match response_rx.await.context("waiting for chunk coordinator")? {
            Ok(chunks) => Ok(chunks),
            Err(error) => bail!("{error}"),
        }
    }
}

struct ChunkBatchRequest {
    chunks: Vec<ChunkSliceRequest>,
    response: oneshot::Sender<ChunkBatchResponse>,
}

type ChunkBatchResponse = std::result::Result<HashMap<crate::ChunkId, Bytes>, String>;

struct ChunkBatch {
    remaining: usize,
    chunks: HashMap<crate::ChunkId, Bytes>,
    response: oneshot::Sender<ChunkBatchResponse>,
}

struct ChunkWaiter {
    batch_id: u64,
    range: Range<usize>,
}

struct InFlightChunk {
    waiters: Vec<ChunkWaiter>,
}

struct CompletedChunkGroup {
    hints: Vec<crate::ChunkLocationHint>,
    result: ChunkFetchResponse,
}

type ChunkFetchResponse = std::result::Result<HashMap<crate::ChunkId, Bytes>, String>;

async fn run_chunk_coordinator<S: BlobStore>(
    store: S,
    cache: LocalCache,
    read_config: ReadConfig,
    pack_data_end: Arc<HashMap<String, u64>>,
    tree_id: TreeId,
    fetch_permits: Arc<Semaphore>,
    mut requests: mpsc::Receiver<ChunkBatchRequest>,
    completed_tx: mpsc::Sender<CompletedChunkGroup>,
    mut completed_rx: mpsc::Receiver<CompletedChunkGroup>,
) {
    let mut in_flight = HashMap::<crate::ChunkId, InFlightChunk>::new();
    let mut batches = HashMap::<u64, ChunkBatch>::new();
    let mut next_batch_id = 0u64;

    loop {
        tokio::select! {
            Some(request) = requests.recv() => {
                handle_chunk_request(
                    request,
                    &mut next_batch_id,
                    &mut batches,
                    &mut in_flight,
                    store.clone(),
                    cache.clone(),
                    read_config,
                    pack_data_end.clone(),
                    tree_id,
                    fetch_permits.clone(),
                    completed_tx.clone(),
                ).await;
            }
            Some(completed) = completed_rx.recv() => {
                handle_chunk_completion(completed, &mut batches, &mut in_flight);
            }
            else => break,
        }
    }
}

async fn handle_chunk_request<S: BlobStore>(
    request: ChunkBatchRequest,
    next_batch_id: &mut u64,
    batches: &mut HashMap<u64, ChunkBatch>,
    in_flight: &mut HashMap<crate::ChunkId, InFlightChunk>,
    store: S,
    cache: LocalCache,
    read_config: ReadConfig,
    pack_data_end: Arc<HashMap<String, u64>>,
    tree_id: TreeId,
    fetch_permits: Arc<Semaphore>,
    completed_tx: mpsc::Sender<CompletedChunkGroup>,
) {
    let batch_id = *next_batch_id;
    *next_batch_id = next_batch_id.wrapping_add(1);
    let remaining = request.chunks.len();
    batches.insert(
        batch_id,
        ChunkBatch {
            remaining,
            chunks: HashMap::new(),
            response: request.response,
        },
    );

    if remaining == 0 {
        finish_batch_if_ready(batch_id, batches);
        return;
    }

    let mut new_hints = Vec::new();
    for request in request.chunks {
        let chunk_id = request.hint.chunk_id;
        match cache.get_chunk(chunk_id).await {
            Ok(Some(bytes)) => {
                debug!(
                    target: "protostore::reader",
                    tree_id = %tree_id,
                    chunk_id = %chunk_id,
                    uncompressed_len = bytes.len(),
                    "chunk coordinator cache hit"
                );
                satisfy_chunk_waiter(batch_id, chunk_id, &request.range, &bytes, batches);
            }
            Ok(None) => {
                if let Some(in_flight) = in_flight.get_mut(&chunk_id) {
                    debug!(
                        target: "protostore::reader",
                        tree_id = %tree_id,
                        chunk_id = %chunk_id,
                        "join in-flight chunk fetch"
                    );
                    in_flight.waiters.push(ChunkWaiter {
                        batch_id,
                        range: request.range,
                    });
                } else {
                    debug!(
                        target: "protostore::reader",
                        tree_id = %tree_id,
                        chunk_id = %chunk_id,
                        "start in-flight chunk fetch"
                    );
                    in_flight.insert(
                        chunk_id,
                        InFlightChunk {
                            waiters: vec![ChunkWaiter {
                                batch_id,
                                range: request.range,
                            }],
                        },
                    );
                    new_hints.push(request.hint);
                }
            }
            Err(error) => {
                fail_batch(batch_id, batches, format!("{error:#}"));
                return;
            }
        }
    }

    if !new_hints.is_empty() {
        let hints = new_hints.clone();
        tokio::spawn(async move {
            let result = match fetch_permits.acquire_owned().await {
                Ok(_permit) => {
                    fetch_chunk_group(store, cache, read_config, pack_data_end, tree_id, hints)
                        .await
                        .map_err(|error| format!("{error:#}"))
                }
                Err(error) => Err(format!("chunk fetch limiter closed: {error}")),
            };
            let _ = completed_tx
                .send(CompletedChunkGroup {
                    hints: new_hints,
                    result,
                })
                .await;
        });
    }
}

fn handle_chunk_completion(
    completed: CompletedChunkGroup,
    batches: &mut HashMap<u64, ChunkBatch>,
    in_flight: &mut HashMap<crate::ChunkId, InFlightChunk>,
) {
    match completed.result {
        Ok(chunks) => {
            for hint in completed.hints {
                let Some(bytes) = chunks.get(&hint.chunk_id) else {
                    fail_waiters(
                        hint.chunk_id,
                        in_flight,
                        batches,
                        format!("chunk fetch finished without {}", hint.chunk_id),
                    );
                    continue;
                };
                if let Some(in_flight) = in_flight.remove(&hint.chunk_id) {
                    for waiter in in_flight.waiters {
                        satisfy_chunk_waiter(
                            waiter.batch_id,
                            hint.chunk_id,
                            &waiter.range,
                            bytes,
                            batches,
                        );
                    }
                }
            }
        }
        Err(error) => {
            for hint in completed.hints {
                fail_waiters(hint.chunk_id, in_flight, batches, error.clone());
            }
        }
    }
}

fn satisfy_chunk_waiter(
    batch_id: u64,
    chunk_id: crate::ChunkId,
    range: &Range<usize>,
    bytes: &Bytes,
    batches: &mut HashMap<u64, ChunkBatch>,
) {
    let Some(batch) = batches.get_mut(&batch_id) else {
        return;
    };
    let Some(slice) = bytes.get(range.clone()) else {
        fail_batch(
            batch_id,
            batches,
            format!(
                "chunk range {}..{} is outside {chunk_id}",
                range.start, range.end
            ),
        );
        return;
    };
    batch.chunks.insert(chunk_id, Bytes::copy_from_slice(slice));
    batch.remaining = batch.remaining.saturating_sub(1);
    finish_batch_if_ready(batch_id, batches);
}

fn fail_waiters(
    chunk_id: crate::ChunkId,
    in_flight: &mut HashMap<crate::ChunkId, InFlightChunk>,
    batches: &mut HashMap<u64, ChunkBatch>,
    error: String,
) {
    if let Some(in_flight) = in_flight.remove(&chunk_id) {
        for waiter in in_flight.waiters {
            fail_batch(waiter.batch_id, batches, error.clone());
        }
    }
}

fn fail_batch(batch_id: u64, batches: &mut HashMap<u64, ChunkBatch>, error: String) {
    if let Some(batch) = batches.remove(&batch_id) {
        let _ = batch.response.send(Err(error));
    }
}

fn finish_batch_if_ready(batch_id: u64, batches: &mut HashMap<u64, ChunkBatch>) {
    let ready = batches
        .get(&batch_id)
        .map(|batch| batch.remaining == 0)
        .unwrap_or(false);
    if ready && let Some(batch) = batches.remove(&batch_id) {
        let _ = batch.response.send(Ok(batch.chunks));
    }
}

async fn fetch_chunk_group<S: BlobStore>(
    store: S,
    cache: LocalCache,
    read_config: ReadConfig,
    pack_data_end: Arc<HashMap<String, u64>>,
    tree_id: TreeId,
    group: Vec<crate::ChunkLocationHint>,
) -> Result<HashMap<crate::ChunkId, Bytes>> {
    let first = group.first().context("empty chunk fetch group")?;
    let last = group.last().context("empty chunk fetch group")?;
    let range_start = first.compressed_offset;
    let needed_end = last.compressed_offset + u64::from(last.compressed_len);
    let pack_data_end = pack_data_end
        .get(&first.pack_key)
        .copied()
        .unwrap_or(needed_end);
    let min_end = range_start
        .saturating_add(read_config.min_remote_read as u64)
        .min(pack_data_end);
    let range_end = needed_end.max(min_end);
    let range_len = range_end - range_start;
    info!(
        target: "protostore::reader",
        tree_id = %tree_id,
        pack_key = %first.pack_key,
        pack_hash = %first.pack_hash,
        compressed_offset = range_start,
        compressed_len = range_len,
        chunk_count = group.len(),
        target_coalesce = read_config.target_coalesce,
        min_remote_read = read_config.min_remote_read,
        "fetch coalesced compressed chunk range"
    );
    let compressed_range = store
        .get_range(&first.pack_key, range_start, range_len)
        .await?;

    let mut out = HashMap::new();
    for hint in group {
        let start = usize::try_from(hint.compressed_offset - range_start)
            .context("coalesced range offset overflow")?;
        let end = start
            .checked_add(hint.compressed_len as usize)
            .context("compressed chunk range overflow")?;
        let compressed = compressed_range.slice(start..end);
        debug!(
            target: "protostore::reader",
            tree_id = %tree_id,
            chunk_id = %hint.chunk_id,
            compressed_len = compressed.len(),
            "decompress chunk"
        );
        let decompressed = decompress_chunk(&compressed)?;
        ensure!(
            decompressed.len() == hint.uncompressed_len as usize,
            "decompressed chunk length mismatch"
        );
        debug!(
            target: "protostore::reader",
            tree_id = %tree_id,
            chunk_id = %hint.chunk_id,
            uncompressed_len = decompressed.len(),
            "cache decompressed chunk"
        );
        cache.put_chunk(hint.chunk_id, &decompressed).await?;
        out.insert(hint.chunk_id, Bytes::from(decompressed));
    }
    Ok(out)
}
