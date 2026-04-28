use anyhow::{Context, Result};
use fuser::{
    BackgroundSession, FileAttr, FileType, Filesystem, KernelConfig, MountOption, ReplyAttr,
    ReplyData, ReplyDirectory, ReplyEntry, ReplyOpen, Request,
};
use protostore_core::{BlobStore, LocalCache, ReadConfig, TreeManifest, TreeReader};
use std::{
    collections::{BTreeMap, HashMap},
    ffi::{OsStr, OsString},
    path::Path,
    time::{Duration, SystemTime},
};
use tokio::runtime::Handle;
use tracing::{debug, info, warn};

const ROOT_INO: u64 = 1;
const IMMUTABLE_TTL: Duration = Duration::from_secs(365 * 24 * 60 * 60);
const DEFAULT_MAX_BACKGROUND: u16 = 64;
const DEFAULT_CONGESTION_THRESHOLD: u16 = 48;

#[derive(Debug, Clone, Copy)]
pub struct FuseTuning {
    pub ttl: Duration,
    pub blksize: u32,
    pub max_readahead: u32,
    pub max_background: u16,
    pub congestion_threshold: u16,
    pub keep_cache: bool,
}

impl FuseTuning {
    pub fn immutable(read_config: ReadConfig) -> Self {
        Self {
            ttl: IMMUTABLE_TTL,
            blksize: clamp_u32(read_config.min_remote_read),
            max_readahead: clamp_u32(read_config.target_coalesce),
            max_background: DEFAULT_MAX_BACKGROUND,
            congestion_threshold: DEFAULT_CONGESTION_THRESHOLD,
            keep_cache: true,
        }
    }
}

impl Default for FuseTuning {
    fn default() -> Self {
        Self::immutable(ReadConfig::default())
    }
}

fn clamp_u32(value: usize) -> u32 {
    value.min(u32::MAX as usize) as u32
}

pub struct ProtoStoreFuseBuilder<S> {
    store: S,
    key: String,
    cache: LocalCache,
    read_config: ReadConfig,
    runtime: Option<Handle>,
    fs_name: String,
    default_permissions: bool,
    fuse_tuning: Option<FuseTuning>,
}

impl<S: BlobStore> ProtoStoreFuseBuilder<S> {
    pub fn new(store: S, key: impl Into<String>) -> Self {
        Self {
            store,
            key: key.into(),
            cache: LocalCache::disposable_default(),
            read_config: ReadConfig::default(),
            runtime: None,
            fs_name: "protostore".to_string(),
            default_permissions: true,
            fuse_tuning: None,
        }
    }

    pub fn runtime_handle(mut self, runtime: Handle) -> Self {
        self.runtime = Some(runtime);
        self
    }

    pub fn cache(mut self, cache: LocalCache) -> Self {
        self.cache = cache;
        self
    }

    pub fn read_config(mut self, read_config: ReadConfig) -> Self {
        self.read_config = read_config;
        self
    }

    pub fn fs_name(mut self, fs_name: impl Into<String>) -> Self {
        self.fs_name = fs_name.into();
        self
    }

    pub fn default_permissions(mut self, default_permissions: bool) -> Self {
        self.default_permissions = default_permissions;
        self
    }

    pub fn fuse_tuning(mut self, fuse_tuning: FuseTuning) -> Self {
        self.fuse_tuning = Some(fuse_tuning);
        self
    }

    fn into_filesystem(self) -> Result<(ProtoStoreFs<S>, Vec<MountOption>)> {
        let runtime = self
            .runtime
            .context("FUSE builder requires a Tokio runtime handle")?;
        let read_config = self.read_config;
        let fuse_tuning = self
            .fuse_tuning
            .unwrap_or_else(|| FuseTuning::immutable(read_config));
        let reader = runtime
            .block_on(TreeReader::open_with_config(
                self.store,
                self.key,
                self.cache,
                read_config,
            ))
            .context("opening tree reader for FUSE")?;
        let reader = BlockingTreeReader::new(runtime, reader);
        let fs = ProtoStoreFs::new(reader, fuse_tuning);
        let mut options = vec![MountOption::RO, MountOption::FSName(self.fs_name)];
        if self.default_permissions {
            options.push(MountOption::DefaultPermissions);
        }
        Ok((fs, options))
    }

    async fn into_filesystem_async(self) -> Result<(ProtoStoreFs<S>, Vec<MountOption>)> {
        let runtime = self
            .runtime
            .context("FUSE builder requires a Tokio runtime handle")?;
        let read_config = self.read_config;
        let fuse_tuning = self
            .fuse_tuning
            .unwrap_or_else(|| FuseTuning::immutable(read_config));
        let reader = TreeReader::open_with_config(self.store, self.key, self.cache, read_config)
            .await
            .context("opening tree reader for FUSE")?;
        let reader = BlockingTreeReader::new(runtime, reader);
        let fs = ProtoStoreFs::new(reader, fuse_tuning);
        let mut options = vec![MountOption::RO, MountOption::FSName(self.fs_name)];
        if self.default_permissions {
            options.push(MountOption::DefaultPermissions);
        }
        Ok((fs, options))
    }

    pub fn mount(self, mountpoint: &Path) -> Result<()> {
        let (fs, options) = self.into_filesystem()?;
        fuser::mount2(fs, mountpoint, &options)
            .with_context(|| format!("mounting {}", mountpoint.display()))
    }

    pub fn spawn(self, mountpoint: &Path) -> Result<BackgroundSession>
    where
        S: Send + 'static,
    {
        let (fs, options) = self.into_filesystem()?;
        fuser::spawn_mount2(fs, mountpoint, &options)
            .with_context(|| format!("mounting {}", mountpoint.display()))
    }

    pub async fn spawn_async(self, mountpoint: &Path) -> Result<BackgroundSession>
    where
        S: Send + 'static,
    {
        let (fs, options) = self.into_filesystem_async().await?;
        fuser::spawn_mount2(fs, mountpoint, &options)
            .with_context(|| format!("mounting {}", mountpoint.display()))
    }
}

pub fn mount_readonly_with_runtime<S: BlobStore>(
    runtime: Handle,
    store: S,
    key: impl Into<String>,
    mountpoint: &Path,
    cache: LocalCache,
    read_config: ReadConfig,
) -> Result<()> {
    ProtoStoreFuseBuilder::new(store, key)
        .runtime_handle(runtime)
        .cache(cache)
        .read_config(read_config)
        .mount(mountpoint)
}

#[derive(Clone)]
struct Node {
    ino: u64,
    parent: u64,
    path: String,
    kind: FileType,
    size: u64,
    mode: u16,
    children: BTreeMap<OsString, u64>,
}

struct ProtoStoreFs<S> {
    reader: BlockingTreeReader<S>,
    nodes: HashMap<u64, Node>,
    by_parent_name: HashMap<(u64, OsString), u64>,
    fuse_tuning: FuseTuning,
}

impl<S: BlobStore> ProtoStoreFs<S> {
    fn new(reader: BlockingTreeReader<S>, fuse_tuning: FuseTuning) -> Self {
        let mut nodes = HashMap::new();
        let mut by_parent_name = HashMap::new();
        nodes.insert(
            ROOT_INO,
            Node {
                ino: ROOT_INO,
                parent: ROOT_INO,
                path: String::new(),
                kind: FileType::Directory,
                size: 0,
                mode: 0o755,
                children: BTreeMap::new(),
            },
        );
        let mut next_ino = ROOT_INO + 1;

        for file in &reader.tree().files {
            let mut parent = ROOT_INO;
            let mut current_path = String::new();
            let parts: Vec<_> = file.path.split('/').collect();
            for (idx, part) in parts.iter().enumerate() {
                let name = OsString::from(part);
                let is_file = idx == parts.len() - 1;
                if let Some(ino) = by_parent_name.get(&(parent, name.clone())).copied() {
                    parent = ino;
                    if !current_path.is_empty() {
                        current_path.push('/');
                    }
                    current_path.push_str(part);
                    continue;
                }
                if !current_path.is_empty() {
                    current_path.push('/');
                }
                current_path.push_str(part);
                let ino = next_ino;
                next_ino += 1;
                let node = Node {
                    ino,
                    parent,
                    path: current_path.clone(),
                    kind: if is_file {
                        FileType::RegularFile
                    } else {
                        FileType::Directory
                    },
                    size: if is_file { file.size } else { 0 },
                    mode: if is_file {
                        (file.mode & 0o777) as u16
                    } else {
                        0o755
                    },
                    children: BTreeMap::new(),
                };
                nodes
                    .get_mut(&parent)
                    .unwrap()
                    .children
                    .insert(name.clone(), ino);
                by_parent_name.insert((parent, name), ino);
                nodes.insert(ino, node);
                parent = ino;
            }
        }
        Self {
            reader,
            nodes,
            by_parent_name,
            fuse_tuning,
        }
    }

    fn attr(&self, node: &Node) -> FileAttr {
        FileAttr {
            ino: node.ino,
            size: node.size,
            blocks: node.size.div_ceil(512),
            atime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
            crtime: SystemTime::UNIX_EPOCH,
            kind: node.kind,
            perm: node.mode,
            nlink: if node.kind == FileType::Directory {
                2
            } else {
                1
            },
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
            blksize: self.fuse_tuning.blksize,
        }
    }
}

impl<S: BlobStore> Filesystem for ProtoStoreFs<S> {
    fn init(&mut self, _req: &Request<'_>, config: &mut KernelConfig) -> Result<(), libc::c_int> {
        let tuning = self.fuse_tuning;
        match config.set_max_readahead(tuning.max_readahead) {
            Ok(previous) => {
                info!(
                    target: "protostore::fuse",
                    requested = tuning.max_readahead,
                    previous,
                    "configured FUSE max readahead"
                );
            }
            Err(max_supported) => {
                let applied = max_supported.max(1);
                let _ = config.set_max_readahead(applied);
                warn!(
                    target: "protostore::fuse",
                    requested = tuning.max_readahead,
                    applied,
                    "kernel capped FUSE max readahead"
                );
            }
        }

        if let Err(nearest) = config.set_max_background(tuning.max_background) {
            warn!(
                target: "protostore::fuse",
                requested = tuning.max_background,
                nearest,
                "kernel rejected FUSE max background request"
            );
        }
        if let Err(nearest) = config.set_congestion_threshold(tuning.congestion_threshold) {
            warn!(
                target: "protostore::fuse",
                requested = tuning.congestion_threshold,
                nearest,
                "kernel rejected FUSE congestion threshold request"
            );
        }
        info!(
            target: "protostore::fuse",
            ttl_secs = tuning.ttl.as_secs(),
            blksize = tuning.blksize,
            keep_cache = tuning.keep_cache,
            "configured immutable FUSE mount"
        );
        Ok(())
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let key = (parent, name.to_os_string());
        if let Some(ino) = self.by_parent_name.get(&key).copied() {
            if let Some(node) = self.nodes.get(&ino) {
                debug!(
                    target: "protostore::fuse",
                    parent,
                    ino,
                    name = %name.to_string_lossy(),
                    "lookup"
                );
                reply.entry(&self.fuse_tuning.ttl, &self.attr(node), 0);
                return;
            }
        }
        reply.error(libc::ENOENT);
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if let Some(node) = self.nodes.get(&ino) {
            debug!(target: "protostore::fuse", ino, "getattr");
            reply.attr(&self.fuse_tuning.ttl, &self.attr(node));
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let Some(node) = self.nodes.get(&ino) else {
            reply.error(libc::ENOENT);
            return;
        };
        if node.kind != FileType::Directory {
            reply.error(libc::ENOTDIR);
            return;
        }
        let mut entries = vec![
            (node.ino, FileType::Directory, OsString::from(".")),
            (node.parent, FileType::Directory, OsString::from("..")),
        ];
        for (name, child_ino) in &node.children {
            let child = self.nodes.get(child_ino).unwrap();
            entries.push((*child_ino, child.kind, name.clone()));
        }
        for (idx, (entry_ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize)
        {
            if reply.add(entry_ino, (idx + 1) as i64, kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        match self.nodes.get(&ino) {
            Some(node) if node.kind == FileType::RegularFile => {
                let flags = if self.fuse_tuning.keep_cache {
                    fuser::consts::FOPEN_KEEP_CACHE
                } else {
                    0
                };
                debug!(
                    target: "protostore::fuse",
                    ino,
                    path = %node.path,
                    keep_cache = self.fuse_tuning.keep_cache,
                    "open"
                );
                reply.opened(0, flags);
            }
            Some(_) => reply.error(libc::EISDIR),
            None => reply.error(libc::ENOENT),
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        let Some(node) = self.nodes.get(&ino).cloned() else {
            reply.error(libc::ENOENT);
            return;
        };
        if node.kind != FileType::RegularFile {
            reply.error(libc::EISDIR);
            return;
        }
        debug!(
            target: "protostore::fuse",
            ino,
            path = %node.path,
            offset,
            size,
            "read"
        );
        match self
            .reader
            .read_at(&node.path, offset as u64, size as usize)
        {
            Ok(bytes) => reply.data(&bytes),
            Err(_) => reply.error(libc::EIO),
        }
    }
}

struct BlockingTreeReader<S> {
    runtime: Handle,
    reader: TreeReader<S>,
}

impl<S: BlobStore> BlockingTreeReader<S> {
    fn new(runtime: Handle, reader: TreeReader<S>) -> Self {
        Self { runtime, reader }
    }

    fn tree(&self) -> &TreeManifest {
        self.reader.tree()
    }

    fn read_at(&self, path: &str, offset: u64, len: usize) -> Result<bytes::Bytes> {
        self.runtime
            .block_on(self.reader.read_at(path, offset, len))
    }
}
