use anyhow::{Context, Result, bail};
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use google_cloud_storage::{
    client::Storage,
    model_ext::ReadRange,
    streaming_source::{SizeHint, StreamingSource},
};
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, local::LocalFileSystem, path::Path as ObjectPath,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
};
use tokio::{io::AsyncWriteExt, task::JoinHandle};
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
    inner: StoreBackend,
}

#[derive(Clone)]
enum StoreBackend {
    Local(LocalBlobStore),
    Gcs(GcsBlobStore),
}

impl ObjectBlobStore {
    pub async fn from_uri(uri: &str) -> Result<Self> {
        if let Some(prefix) = uri.strip_prefix("file://") {
            return Ok(Self {
                inner: StoreBackend::Local(LocalBlobStore::from_file_uri(prefix)?),
            });
        }

        if let Some(rest) = uri.strip_prefix("gs://") {
            return Ok(Self {
                inner: StoreBackend::Gcs(GcsBlobStore::from_gcs_uri(rest).await?),
            });
        }

        bail!("unsupported store URI {uri}; expected file:// or gs://")
    }

    pub fn local(root: impl Into<PathBuf>) -> Result<Self> {
        Ok(Self {
            inner: StoreBackend::Local(LocalBlobStore::new(root.into())?),
        })
    }
}

#[async_trait]
impl BlobStore for ObjectBlobStore {
    async fn put_bytes(&self, key: &str, bytes: Bytes) -> Result<()> {
        match &self.inner {
            StoreBackend::Local(store) => store.put_bytes(key, bytes).await,
            StoreBackend::Gcs(store) => store.put_bytes(key, bytes).await,
        }
    }

    async fn begin_multipart(&self, key: &str, part_size: usize) -> Result<Box<dyn BlobUpload>> {
        match &self.inner {
            StoreBackend::Local(store) => store.begin_multipart(key, part_size).await,
            StoreBackend::Gcs(store) => store.begin_multipart(key, part_size).await,
        }
    }

    async fn get_bytes(&self, key: &str) -> Result<Bytes> {
        match &self.inner {
            StoreBackend::Local(store) => store.get_bytes(key).await,
            StoreBackend::Gcs(store) => store.get_bytes(key).await,
        }
    }

    async fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Bytes> {
        match &self.inner {
            StoreBackend::Local(store) => store.get_range(key, offset, len).await,
            StoreBackend::Gcs(store) => store.get_range(key, offset, len).await,
        }
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        match &self.inner {
            StoreBackend::Local(store) => store.exists(key).await,
            StoreBackend::Gcs(store) => store.exists(key).await,
        }
    }
}

#[derive(Clone)]
struct LocalBlobStore {
    inner: Arc<dyn ObjectStore>,
    root: Arc<PathBuf>,
}

impl LocalBlobStore {
    fn from_file_uri(prefix: &str) -> Result<Self> {
        if prefix.is_empty() {
            bail!("file:// store URI must include a path");
        }
        Self::new(PathBuf::from(prefix))
    }

    fn new(root: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&root)
            .with_context(|| format!("creating local object store {}", root.display()))?;
        let fs = LocalFileSystem::new_with_prefix(&root)
            .context("creating local object store with prefix")?;
        Ok(Self {
            inner: Arc::new(fs),
            root: Arc::new(root),
        })
    }

    fn object_path(&self, key: &str) -> Result<ObjectPath> {
        validate_object_key(key)?;
        Ok(ObjectPath::from(key))
    }
}

#[async_trait]
impl BlobStore for LocalBlobStore {
    async fn put_bytes(&self, key: &str, bytes: Bytes) -> Result<()> {
        self.inner
            .put(&self.object_path(key)?, PutPayload::from(bytes))
            .await
            .with_context(|| format!("put local object {key}"))?;
        Ok(())
    }

    async fn begin_multipart(&self, key: &str, _part_size: usize) -> Result<Box<dyn BlobUpload>> {
        let path = local_object_path(&self.root, key)?;
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
        Ok(Box::new(LocalFileUpload { path, file }))
    }

    async fn get_bytes(&self, key: &str) -> Result<Bytes> {
        let result = self
            .inner
            .get(&self.object_path(key)?)
            .await
            .with_context(|| format!("get local object {key}"))?;
        result
            .bytes()
            .await
            .with_context(|| format!("read local object {key}"))
    }

    async fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Bytes> {
        let end = offset.checked_add(len).context("range end overflow")?;
        debug!(
            target: "protostore::object_store",
            key,
            offset,
            len,
            end,
            "local object range read"
        );
        self.inner
            .get_range(&self.object_path(key)?, offset..end)
            .await
            .with_context(|| format!("range read local object {key} at {offset}+{len}"))
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        match self.inner.head(&self.object_path(key)?).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(err).with_context(|| format!("head local object {key}")),
        }
    }
}

#[derive(Clone)]
struct GcsBlobStore {
    client: Storage,
    bucket: Arc<str>,
    bucket_resource: Arc<str>,
    key_prefix: Arc<str>,
}

impl GcsBlobStore {
    async fn from_gcs_uri(rest: &str) -> Result<Self> {
        let (bucket, prefix) = match rest.split_once('/') {
            Some((bucket, prefix)) => (bucket, prefix),
            None => (rest, ""),
        };
        if bucket.is_empty() {
            bail!("gs:// store URI must include a bucket name");
        }

        install_rustls_crypto_provider();
        let client = Storage::builder()
            .build()
            .await
            .with_context(|| format!("creating official GCS storage client for bucket {bucket}"))?;

        Ok(Self {
            client,
            bucket: Arc::from(bucket),
            bucket_resource: Arc::from(format!("projects/_/buckets/{bucket}")),
            key_prefix: Arc::from(normalize_object_prefix(prefix)?),
        })
    }

    fn object_key(&self, key: &str) -> Result<String> {
        prefixed_object_key(&self.key_prefix, key)
    }
}

#[async_trait]
impl BlobStore for GcsBlobStore {
    async fn put_bytes(&self, key: &str, bytes: Bytes) -> Result<()> {
        let object_key = self.object_key(key)?;
        self.client
            .write_object(self.bucket_resource.as_ref(), object_key.as_str(), bytes)
            .send_unbuffered()
            .await
            .with_context(|| format!("put GCS object gs://{}/{}", self.bucket, object_key))?;
        Ok(())
    }

    async fn begin_multipart(&self, key: &str, _part_size: usize) -> Result<Box<dyn BlobUpload>> {
        let object_key = self.object_key(key)?;
        let (tx, rx) = async_channel::bounded::<Bytes>(4);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let bucket_resource = self.bucket_resource.clone();
        let upload_key = object_key.clone();
        let handle = tokio::spawn(async move {
            client
                .write_object(
                    bucket_resource.as_ref(),
                    upload_key.as_str(),
                    ChannelSource { rx },
                )
                .send_buffered()
                .await
                .with_context(|| format!("finish GCS upload gs://{bucket}/{upload_key}"))?;
            Ok(())
        });
        Ok(Box::new(GcsUpload {
            key: object_key,
            tx: Some(tx),
            handle,
        }))
    }

    async fn get_bytes(&self, key: &str) -> Result<Bytes> {
        let object_key = self.object_key(key)?;
        self.read_object_range(&object_key, ReadRange::all()).await
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
            "GCS object range read"
        );
        self.read_object_range(&object_key, ReadRange::segment(offset, len))
            .await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let object_key = self.object_key(key)?;
        match self
            .read_object_range(&object_key, ReadRange::head(1))
            .await
        {
            Ok(_) => Ok(true),
            Err(err) if is_gcs_not_found(&err) => Ok(false),
            Err(err) => Err(err),
        }
    }
}

impl GcsBlobStore {
    async fn read_object_range(&self, object_key: &str, range: ReadRange) -> Result<Bytes> {
        let mut response = self
            .client
            .read_object(self.bucket_resource.as_ref(), object_key)
            .set_read_range(range)
            .send()
            .await
            .with_context(|| format!("read GCS object gs://{}/{}", self.bucket, object_key))?;
        let mut out = BytesMut::new();
        while let Some(chunk) = response.next().await {
            let chunk = chunk.with_context(|| {
                format!("stream GCS object gs://{}/{}", self.bucket, object_key)
            })?;
            out.extend_from_slice(&chunk);
        }
        Ok(out.freeze())
    }
}

fn install_rustls_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }
}

fn is_gcs_not_found(err: &anyhow::Error) -> bool {
    err.chain().any(|source| {
        source
            .downcast_ref::<google_cloud_storage::Error>()
            .and_then(google_cloud_storage::Error::http_status_code)
            == Some(404)
    })
}

#[derive(Debug)]
struct ChannelSource {
    rx: Receiver<Bytes>,
}

impl StreamingSource for ChannelSource {
    type Error = async_channel::RecvError;

    async fn next(&mut self) -> Option<Result<Bytes, Self::Error>> {
        self.rx.recv().await.map(Ok).ok()
    }

    async fn size_hint(&self) -> Result<SizeHint, Self::Error> {
        Ok(SizeHint::new())
    }
}

struct GcsUpload {
    key: String,
    tx: Option<Sender<Bytes>>,
    handle: JoinHandle<Result<()>>,
}

#[async_trait]
impl BlobUpload for GcsUpload {
    async fn put(&mut self, bytes: Bytes) -> Result<()> {
        let tx = self.tx.as_ref().context("GCS upload is already closed")?;
        tx.send(bytes)
            .await
            .with_context(|| format!("send bytes to GCS upload {}", self.key))
    }

    async fn finish(mut self: Box<Self>) -> Result<()> {
        drop(self.tx.take());
        self.handle
            .await
            .with_context(|| format!("join GCS upload {}", self.key))?
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        if let Some(tx) = self.tx.take() {
            tx.close();
        }
        self.handle.abort();
        Ok(())
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

fn prefixed_object_key(prefix: &str, key: &str) -> Result<String> {
    validate_object_key(key)?;
    if prefix.is_empty() {
        Ok(key.to_owned())
    } else {
        Ok(format!("{prefix}/{key}"))
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
    fn prefixes_gcs_object_keys() {
        assert_eq!(
            prefixed_object_key("cache/protostore", "trees/example.tree").unwrap(),
            "cache/protostore/trees/example.tree"
        );
        assert!(prefixed_object_key("cache/protostore", "../trees/example.tree").is_err());
    }
}
