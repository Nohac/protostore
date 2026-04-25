use crate::{
    cache::LocalCache,
    hash::TreeId,
    pack::decompress_chunk,
    profile::ProfileRecorder,
    store::BlobStore,
    tree::{
        FileNode, LayoutManifest, TreeManifest, load_layout, load_tree, location_map,
        safe_output_path, write_file_mode,
    },
};
use anyhow::{Context, Result, bail, ensure};
use bytes::{Bytes, BytesMut};
use std::{collections::HashMap, path::Path};
use tokio::{fs, io::AsyncWriteExt};
use tracing::{debug, info};

#[derive(Clone)]
pub struct TreeReader<S> {
    store: S,
    tree_id: TreeId,
    tree: TreeManifest,
    layout: LayoutManifest,
    files: HashMap<String, FileNode>,
    locations: HashMap<crate::ChunkId, crate::ChunkLocationHint>,
    pack_data_end: HashMap<String, u64>,
    cache: LocalCache,
    recorder: Option<ProfileRecorder>,
    read_config: ReadConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadConfig {
    pub min_remote_read: usize,
    pub target_coalesce: usize,
}

impl Default for ReadConfig {
    fn default() -> Self {
        Self {
            min_remote_read: 16 * 1024 * 1024,
            target_coalesce: 64 * 1024 * 1024,
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
        Ok(self)
    }
}

impl<S: BlobStore> TreeReader<S> {
    pub async fn open(store: S, tree_id: TreeId, cache: LocalCache) -> Result<Self> {
        Self::open_with_config(store, tree_id, cache, ReadConfig::default()).await
    }

    pub async fn open_with_config(
        store: S,
        tree_id: TreeId,
        cache: LocalCache,
        read_config: ReadConfig,
    ) -> Result<Self> {
        let tree = load_tree(&store, tree_id).await?;
        let layout = load_layout(&store, tree.layout_id).await?;
        ensure!(
            layout.tree_id == tree_id,
            "layout {} belongs to tree {}, not {}",
            tree.layout_id,
            layout.tree_id,
            tree_id
        );
        Self::from_manifest_with_config(store, tree_id, tree, layout, cache, None, read_config)
    }

    pub fn from_manifest(
        store: S,
        tree_id: TreeId,
        tree: TreeManifest,
        layout: LayoutManifest,
        cache: LocalCache,
        recorder: Option<ProfileRecorder>,
    ) -> Result<Self> {
        Self::from_manifest_with_config(
            store,
            tree_id,
            tree,
            layout,
            cache,
            recorder,
            ReadConfig::default(),
        )
    }

    pub fn from_manifest_with_config(
        store: S,
        tree_id: TreeId,
        tree: TreeManifest,
        layout: LayoutManifest,
        cache: LocalCache,
        recorder: Option<ProfileRecorder>,
        read_config: ReadConfig,
    ) -> Result<Self> {
        let read_config = read_config.validate()?;
        let files = tree
            .files
            .iter()
            .map(|file| (file.path.clone(), file.clone()))
            .collect();
        ensure!(
            layout.tree_id == tree_id,
            "layout {} belongs to tree {}, not {}",
            tree.layout_id,
            layout.tree_id,
            tree_id
        );
        let locations = location_map(&layout);
        let mut pack_data_end = HashMap::<String, u64>::new();
        for hint in &layout.locations {
            let end = hint.compressed_offset + u64::from(hint.compressed_len);
            pack_data_end
                .entry(hint.pack_key.clone())
                .and_modify(|current| *current = (*current).max(end))
                .or_insert(end);
        }
        Ok(Self {
            store,
            tree_id,
            tree,
            layout,
            files,
            locations,
            pack_data_end,
            cache,
            recorder,
            read_config,
        })
    }

    pub fn tree(&self) -> &TreeManifest {
        &self.tree
    }

    pub fn layout(&self) -> &LayoutManifest {
        &self.layout
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
                chunk_uncompressed_len = chunk.uncompressed_len,
                take_start,
                take_end,
                "read_at selected chunk"
            );
            selected.push(SelectedChunk {
                chunk_id: chunk.chunk_id,
                take_start,
                take_end,
            });
        }
        let chunks = self.read_chunks(&selected).await?;
        for selected in selected {
            let chunk_bytes = chunks
                .get(&selected.chunk_id)
                .with_context(|| format!("missing decoded chunk {}", selected.chunk_id))?;
            out.extend_from_slice(&chunk_bytes[selected.take_start..selected.take_end]);
        }
        Ok(out.freeze())
    }

    async fn read_chunks(
        &self,
        selected: &[SelectedChunk],
    ) -> Result<HashMap<crate::ChunkId, Bytes>> {
        let mut out = HashMap::new();
        let mut misses = Vec::new();
        for selected in selected {
            let chunk_id = selected.chunk_id;
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
                out.insert(chunk_id, bytes);
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
                misses.push(hint.clone());
            }
        }

        for group in self.coalesce_misses(misses) {
            self.fetch_group(group, &mut out).await?;
        }

        Ok(out)
    }

    fn coalesce_misses(
        &self,
        mut misses: Vec<crate::ChunkLocationHint>,
    ) -> Vec<Vec<crate::ChunkLocationHint>> {
        misses.sort_by_key(|hint| (hint.pack_key.clone(), hint.compressed_offset, hint.chunk_id));
        let mut groups: Vec<Vec<crate::ChunkLocationHint>> = Vec::new();
        for hint in misses {
            let Some(current) = groups.last_mut() else {
                groups.push(vec![hint]);
                continue;
            };
            let first = current.first().unwrap();
            let last = current.last().unwrap();
            let current_start = first.compressed_offset;
            let current_end = last.compressed_offset + u64::from(last.compressed_len);
            let next_end = hint.compressed_offset + u64::from(hint.compressed_len);
            let coalesced_len = next_end.saturating_sub(current_start);
            if hint.pack_key == first.pack_key
                && hint.compressed_offset >= current_end
                && coalesced_len <= self.read_config.target_coalesce as u64
            {
                current.push(hint);
            } else {
                groups.push(vec![hint]);
            }
        }
        groups
    }

    async fn fetch_group(
        &self,
        group: Vec<crate::ChunkLocationHint>,
        out: &mut HashMap<crate::ChunkId, Bytes>,
    ) -> Result<()> {
        let first = group.first().context("empty chunk fetch group")?;
        let last = group.last().context("empty chunk fetch group")?;
        let range_start = first.compressed_offset;
        let needed_end = last.compressed_offset + u64::from(last.compressed_len);
        let pack_data_end = self
            .pack_data_end
            .get(&first.pack_key)
            .copied()
            .unwrap_or(needed_end);
        let min_end = range_start
            .saturating_add(self.read_config.min_remote_read as u64)
            .min(pack_data_end);
        let range_end = needed_end.max(min_end);
        let range_len = range_end - range_start;
        info!(
            target: "protostore::reader",
            tree_id = %self.tree_id,
            pack_key = %first.pack_key,
            pack_hash = %first.pack_hash,
            compressed_offset = range_start,
            compressed_len = range_len,
            chunk_count = group.len(),
            target_coalesce = self.read_config.target_coalesce,
            min_remote_read = self.read_config.min_remote_read,
            "fetch coalesced compressed chunk range"
        );
        let compressed_range = self
            .store
            .get_range(&first.pack_key, range_start, range_len)
            .await?;

        for hint in group {
            let start = usize::try_from(hint.compressed_offset - range_start)
                .context("coalesced range offset overflow")?;
            let end = start
                .checked_add(hint.compressed_len as usize)
                .context("compressed chunk range overflow")?;
            let compressed = compressed_range.slice(start..end);
            debug!(
                target: "protostore::reader",
                tree_id = %self.tree_id,
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
                tree_id = %self.tree_id,
                chunk_id = %hint.chunk_id,
                uncompressed_len = decompressed.len(),
                "cache decompressed chunk"
            );
            self.cache.put_chunk(hint.chunk_id, &decompressed).await?;
            if let Some(recorder) = &self.recorder {
                recorder.record(self.tree_id, hint.chunk_id);
            }
            out.insert(hint.chunk_id, Bytes::from(decompressed));
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
    take_start: usize,
    take_end: usize,
}
