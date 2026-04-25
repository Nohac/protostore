use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart, local::LocalFileSystem,
    path::Path as ObjectPath,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
};
use tokio::io::AsyncWriteExt;
use tracing::debug;

#[async_trait]
pub trait BlobStore: Send + Sync + Clone + 'static {
    async fn put_bytes(&self, key: &str, bytes: Bytes) -> Result<()>;
    async fn begin_multipart(&self, key: &str, part_size: usize) -> Result<Box<dyn BlobUpload>>;
    async fn get_bytes(&self, key: &str) -> Result<Bytes>;
    async fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Bytes>;
    async fn exists(&self, key: &str) -> Result<bool>;

    async fn put_json<T: Serialize + Sync>(&self, key: &str, value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value).with_context(|| format!("serializing {key}"))?;
        self.put_bytes(key, Bytes::from(bytes)).await
    }

    async fn get_json<T: DeserializeOwned>(&self, key: &str) -> Result<T> {
        let bytes = self.get_bytes(key).await?;
        serde_json::from_slice(&bytes).with_context(|| format!("deserializing {key}"))
    }
}

#[async_trait]
pub trait BlobUpload: Send {
    async fn put(&mut self, bytes: Bytes) -> Result<()>;
    async fn finish(self: Box<Self>) -> Result<()>;
    async fn abort(self: Box<Self>) -> Result<()>;
}

#[derive(Clone)]
pub struct ObjectBlobStore {
    inner: Arc<dyn ObjectStore>,
    local_root: Option<Arc<PathBuf>>,
}

impl ObjectBlobStore {
    pub fn from_uri(uri: &str) -> Result<Self> {
        let prefix = uri
            .strip_prefix("file://")
            .context("only file:// stores are implemented in this MVP")?;
        if prefix.is_empty() {
            bail!("file:// store URI must include a path");
        }
        let root = PathBuf::from(prefix);
        std::fs::create_dir_all(&root)
            .with_context(|| format!("creating local object store {}", root.display()))?;
        let fs = LocalFileSystem::new_with_prefix(&root)
            .context("creating local object store with prefix")?;
        Ok(Self {
            inner: Arc::new(fs),
            local_root: Some(Arc::new(root)),
        })
    }
}

#[async_trait]
impl BlobStore for ObjectBlobStore {
    async fn put_bytes(&self, key: &str, bytes: Bytes) -> Result<()> {
        self.inner
            .put(&ObjectPath::from(key), PutPayload::from(bytes))
            .await
            .with_context(|| format!("put object {key}"))?;
        Ok(())
    }

    async fn begin_multipart(&self, key: &str, part_size: usize) -> Result<Box<dyn BlobUpload>> {
        if let Some(root) = &self.local_root {
            let path = local_object_path(root, key)?;
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .with_context(|| format!("creating parent directory for {key}"))?;
            }
            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&path)
                .await
                .with_context(|| format!("begin direct local upload {key}"))?;
            return Ok(Box::new(LocalFileUpload { path, file }));
        }
        let upload = self
            .inner
            .put_multipart(&ObjectPath::from(key))
            .await
            .with_context(|| format!("begin multipart upload {key}"))?;
        Ok(Box::new(ObjectBlobUpload {
            inner: WriteMultipart::new_with_chunk_size(upload, part_size),
        }))
    }

    async fn get_bytes(&self, key: &str) -> Result<Bytes> {
        let result = self
            .inner
            .get(&ObjectPath::from(key))
            .await
            .with_context(|| format!("get object {key}"))?;
        result
            .bytes()
            .await
            .with_context(|| format!("read object {key}"))
    }

    async fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Bytes> {
        let end = offset.checked_add(len).context("range end overflow")?;
        debug!(
            target: "protostore::object_store",
            key,
            offset,
            len,
            end,
            "object range read"
        );
        self.inner
            .get_range(&ObjectPath::from(key), offset..end)
            .await
            .with_context(|| format!("range read object {key} at {offset}+{len}"))
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        match self.inner.head(&ObjectPath::from(key)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(err).with_context(|| format!("head object {key}")),
        }
    }
}

fn local_object_path(root: &Path, key: &str) -> Result<PathBuf> {
    let key_path = Path::new(key);
    if key_path.is_absolute()
        || key_path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        bail!("unsafe object key {key}");
    }
    Ok(root.join(key_path))
}

struct ObjectBlobUpload {
    inner: WriteMultipart,
}

#[async_trait]
impl BlobUpload for ObjectBlobUpload {
    async fn put(&mut self, bytes: Bytes) -> Result<()> {
        self.inner.wait_for_capacity(4).await?;
        self.inner.put(bytes);
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<()> {
        self.inner.finish().await?;
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        self.inner.abort().await?;
        Ok(())
    }
}

struct LocalFileUpload {
    path: PathBuf,
    file: tokio::fs::File,
}

#[async_trait]
impl BlobUpload for LocalFileUpload {
    async fn put(&mut self, bytes: Bytes) -> Result<()> {
        self.file
            .write_all(&bytes)
            .await
            .with_context(|| format!("write direct local upload {}", self.path.display()))?;
        self.file
            .flush()
            .await
            .with_context(|| format!("flush direct local upload {}", self.path.display()))
    }

    async fn finish(mut self: Box<Self>) -> Result<()> {
        self.file
            .flush()
            .await
            .with_context(|| format!("finish direct local upload {}", self.path.display()))
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        drop(self.file);
        match tokio::fs::remove_file(&self.path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err)
                .with_context(|| format!("abort direct local upload {}", self.path.display())),
        }
    }
}
