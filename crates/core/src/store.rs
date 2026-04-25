use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, local::LocalFileSystem, path::Path as ObjectPath,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{path::PathBuf, sync::Arc};
use tracing::debug;

#[async_trait]
pub trait BlobStore: Send + Sync + Clone + 'static {
    async fn put_bytes(&self, key: &str, bytes: Bytes) -> Result<()>;
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

#[derive(Clone)]
pub struct ObjectBlobStore {
    inner: Arc<dyn ObjectStore>,
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
        let fs = LocalFileSystem::new_with_prefix(root)
            .context("creating local object store with prefix")?;
        Ok(Self {
            inner: Arc::new(fs),
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
