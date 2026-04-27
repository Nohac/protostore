use crate::hash::ChunkId;
use anyhow::{Context, Result};
use bytes::Bytes;
use std::path::{Path, PathBuf};
use tokio::fs;

#[derive(Clone, Debug)]
pub struct LocalCache {
    root: PathBuf,
}

impl LocalCache {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn disposable_default() -> Self {
        Self::new(".protostore-cache")
    }

    fn chunk_path(&self, chunk_id: ChunkId) -> PathBuf {
        self.root.join("chunks").join(chunk_id.to_hex())
    }

    pub async fn get_chunk(&self, chunk_id: ChunkId) -> Result<Option<Bytes>> {
        let path = self.chunk_path(chunk_id);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&path)
            .await
            .with_context(|| format!("reading cached chunk {}", path.display()))?;
        Ok(Some(Bytes::from(bytes)))
    }

    pub async fn contains_chunk(&self, chunk_id: ChunkId) -> Result<bool> {
        let path = self.chunk_path(chunk_id);
        fs::try_exists(&path)
            .await
            .with_context(|| format!("checking cached chunk {}", path.display()))
    }

    pub async fn put_chunk(&self, chunk_id: ChunkId, bytes: &[u8]) -> Result<()> {
        let path = self.chunk_path(chunk_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating cache dir {}", parent.display()))?;
        }
        fs::write(&path, bytes)
            .await
            .with_context(|| format!("writing cached chunk {}", path.display()))?;
        Ok(())
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}
