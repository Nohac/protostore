use crate::{
    cache::LocalCache,
    hash::TreeId,
    pack::{decompress_chunk, pack_key},
    profile::ProfileRecorder,
    store::BlobStore,
    tree::{FileNode, TreeManifest, load_tree, location_map, safe_output_path, write_file_mode},
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
    files: HashMap<String, FileNode>,
    locations: HashMap<crate::ChunkId, crate::ChunkLocationHint>,
    cache: LocalCache,
    recorder: Option<ProfileRecorder>,
}

impl<S: BlobStore> TreeReader<S> {
    pub async fn open(store: S, tree_id: TreeId, cache: LocalCache) -> Result<Self> {
        let tree = load_tree(&store, tree_id).await?;
        Self::from_manifest(store, tree_id, tree, cache, None)
    }

    pub fn from_manifest(
        store: S,
        tree_id: TreeId,
        tree: TreeManifest,
        cache: LocalCache,
        recorder: Option<ProfileRecorder>,
    ) -> Result<Self> {
        let files = tree
            .files
            .iter()
            .map(|file| (file.path.clone(), file.clone()))
            .collect();
        let locations = location_map(&tree);
        Ok(Self {
            store,
            tree_id,
            tree,
            files,
            locations,
            cache,
            recorder,
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
            let chunk_bytes = self.read_chunk(chunk.chunk_id).await?;
            out.extend_from_slice(&chunk_bytes[take_start..take_end]);
        }
        Ok(out.freeze())
    }

    async fn read_chunk(&self, chunk_id: crate::ChunkId) -> Result<Bytes> {
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
            return Ok(bytes);
        }
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
        info!(
            target: "protostore::reader",
            tree_id = %self.tree_id,
            chunk_id = %chunk_id,
            pack_id = %hint.pack_id,
            compressed_offset = hint.compressed_offset,
            compressed_len = hint.compressed_len,
            uncompressed_len = hint.uncompressed_len,
            "fetch compressed chunk range"
        );
        let compressed = self
            .store
            .get_range(
                &pack_key(hint.pack_id),
                hint.compressed_offset,
                u64::from(hint.compressed_len),
            )
            .await?;
        debug!(
            target: "protostore::reader",
            tree_id = %self.tree_id,
            chunk_id = %chunk_id,
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
            chunk_id = %chunk_id,
            uncompressed_len = decompressed.len(),
            "cache decompressed chunk"
        );
        self.cache.put_chunk(chunk_id, &decompressed).await?;
        if let Some(recorder) = &self.recorder {
            recorder.record(self.tree_id, chunk_id);
        }
        Ok(Bytes::from(decompressed))
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
