use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart, gcp::GoogleCloudStorageBuilder,
    local::LocalFileSystem, path::Path as ObjectPath,
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
    key_prefix: Arc<str>,
}

impl ObjectBlobStore {
    pub fn from_uri(uri: &str) -> Result<Self> {
        if let Some(prefix) = uri.strip_prefix("file://") {
            return Self::from_file_uri(prefix);
        }

        if let Some(rest) = uri.strip_prefix("gs://") {
            return Self::from_gcs_uri(rest);
        }

        bail!("unsupported store URI {uri}; expected file:// or gs://")
    }

    fn from_file_uri(prefix: &str) -> Result<Self> {
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
            key_prefix: Arc::from(""),
        })
    }

    fn from_gcs_uri(rest: &str) -> Result<Self> {
        let (bucket, prefix) = match rest.split_once('/') {
            Some((bucket, prefix)) => (bucket, prefix),
            None => (rest, ""),
        };
        if bucket.is_empty() {
            bail!("gs:// store URI must include a bucket name");
        }

        let store = GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(bucket)
            .build()
            .with_context(|| format!("creating GCS object store for bucket {bucket}"))?;

        Ok(Self {
            inner: Arc::new(store),
            local_root: None,
            key_prefix: Arc::from(normalize_object_prefix(prefix)?),
        })
    }

    fn object_key(&self, key: &str) -> Result<String> {
        validate_object_key(key)?;
        if self.key_prefix.is_empty() {
            Ok(key.to_owned())
        } else {
            Ok(format!("{}/{}", self.key_prefix, key))
        }
    }
}

#[async_trait]
impl BlobStore for ObjectBlobStore {
    async fn put_bytes(&self, key: &str, bytes: Bytes) -> Result<()> {
        let object_key = self.object_key(key)?;
        self.inner
            .put(
                &ObjectPath::from(object_key.as_str()),
                PutPayload::from(bytes),
            )
            .await
            .with_context(|| format!("put object {object_key}"))?;
        Ok(())
    }

    async fn begin_multipart(&self, key: &str, part_size: usize) -> Result<Box<dyn BlobUpload>> {
        let object_key = self.object_key(key)?;
        if let Some(root) = &self.local_root {
            let path = local_object_path(root, &object_key)?;
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .with_context(|| format!("creating parent directory for {object_key}"))?;
            }
            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&path)
                .await
                .with_context(|| format!("begin direct local upload {object_key}"))?;
            return Ok(Box::new(LocalFileUpload { path, file }));
        }
        let upload = self
            .inner
            .put_multipart(&ObjectPath::from(object_key.as_str()))
            .await
            .with_context(|| format!("begin multipart upload {object_key}"))?;
        Ok(Box::new(ObjectBlobUpload {
            inner: WriteMultipart::new_with_chunk_size(upload, part_size),
        }))
    }

    async fn get_bytes(&self, key: &str) -> Result<Bytes> {
        let object_key = self.object_key(key)?;
        let result = self
            .inner
            .get(&ObjectPath::from(object_key.as_str()))
            .await
            .with_context(|| format!("get object {object_key}"))?;
        result
            .bytes()
            .await
            .with_context(|| format!("read object {object_key}"))
    }

    async fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Bytes> {
        let object_key = self.object_key(key)?;
        let end = offset.checked_add(len).context("range end overflow")?;
        debug!(
            target: "protostore::object_store",
            key = object_key,
            offset,
            len,
            end,
            "object range read"
        );
        self.inner
            .get_range(&ObjectPath::from(object_key.as_str()), offset..end)
            .await
            .with_context(|| format!("range read object {object_key} at {offset}+{len}"))
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let object_key = self.object_key(key)?;
        match self
            .inner
            .head(&ObjectPath::from(object_key.as_str()))
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(err).with_context(|| format!("head object {object_key}")),
        }
    }
}

fn local_object_path(root: &Path, key: &str) -> Result<PathBuf> {
    validate_object_key(key)?;
    Ok(root.join(Path::new(key)))
}

fn validate_object_key(key: &str) -> Result<()> {
    let key_path = Path::new(key);
    if key.is_empty() {
        bail!("object key must not be empty");
    }
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
    Ok(())
}

fn normalize_object_prefix(prefix: &str) -> Result<String> {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        return Ok(String::new());
    }
    validate_object_key(prefix)?;
    Ok(prefix.to_owned())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_object_prefix() {
        assert_eq!(normalize_object_prefix("").unwrap(), "");
        assert_eq!(
            normalize_object_prefix("/cache/protostore/").unwrap(),
            "cache/protostore"
        );
        assert!(normalize_object_prefix("../cache").is_err());
    }

    #[test]
    fn prefixes_object_keys() {
        let store = ObjectBlobStore {
            inner: Arc::new(LocalFileSystem::new()),
            local_root: None,
            key_prefix: Arc::from("cache/protostore"),
        };

        assert_eq!(
            store.object_key("trees/example.tree").unwrap(),
            "cache/protostore/trees/example.tree"
        );
        assert!(store.object_key("../trees/example.tree").is_err());
    }
}
