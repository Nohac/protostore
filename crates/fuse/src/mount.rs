use anyhow::{Context, Result};
use fuser::{
    BackgroundSession, FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEntry, ReplyOpen, Request,
};
use protostore_core::{BlobStore, LocalCache, TreeId, TreeReader};
use std::{
    collections::{BTreeMap, HashMap},
    ffi::{OsStr, OsString},
    path::Path,
    time::{Duration, SystemTime},
};
use tokio::runtime::Handle;

const TTL: Duration = Duration::from_secs(1);
const ROOT_INO: u64 = 1;

pub struct ProtoStoreFuseBuilder<S> {
    store: S,
    tree_id: TreeId,
    cache: LocalCache,
    runtime: Option<Handle>,
    fs_name: String,
    default_permissions: bool,
}

impl<S: BlobStore> ProtoStoreFuseBuilder<S> {
    pub fn new(store: S, tree_id: TreeId) -> Self {
        Self {
            store,
            tree_id,
            cache: LocalCache::disposable_default(),
            runtime: None,
            fs_name: "protostore".to_string(),
            default_permissions: true,
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

    pub fn fs_name(mut self, fs_name: impl Into<String>) -> Self {
        self.fs_name = fs_name.into();
        self
    }

    pub fn default_permissions(mut self, default_permissions: bool) -> Self {
        self.default_permissions = default_permissions;
        self
    }

    fn into_filesystem(self) -> Result<(ProtoStoreFs<S>, Vec<MountOption>)> {
        let runtime = self
            .runtime
            .context("FUSE builder requires a Tokio runtime handle")?;
        let reader = runtime
            .block_on(TreeReader::open(self.store, self.tree_id, self.cache))
            .context("opening tree reader for FUSE")?;
        let fs = ProtoStoreFs::new(runtime, reader);
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
}

pub fn mount_readonly_with_runtime<S: BlobStore>(
    runtime: Handle,
    store: S,
    tree_id: TreeId,
    mountpoint: &Path,
    cache: LocalCache,
) -> Result<()> {
    ProtoStoreFuseBuilder::new(store, tree_id)
        .runtime_handle(runtime)
        .cache(cache)
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
    runtime: Handle,
    reader: TreeReader<S>,
    nodes: HashMap<u64, Node>,
    by_parent_name: HashMap<(u64, OsString), u64>,
}

impl<S: BlobStore> ProtoStoreFs<S> {
    fn new(runtime: Handle, reader: TreeReader<S>) -> Self {
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
            runtime,
            reader,
            nodes,
            by_parent_name,
        }
    }

    fn attr(node: &Node) -> FileAttr {
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
            blksize: 4096,
        }
    }
}

impl<S: BlobStore> Filesystem for ProtoStoreFs<S> {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let key = (parent, name.to_os_string());
        if let Some(ino) = self.by_parent_name.get(&key).copied() {
            if let Some(node) = self.nodes.get(&ino) {
                reply.entry(&TTL, &Self::attr(node), 0);
                return;
            }
        }
        reply.error(libc::ENOENT);
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if let Some(node) = self.nodes.get(&ino) {
            reply.attr(&TTL, &Self::attr(node));
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
            Some(node) if node.kind == FileType::RegularFile => reply.opened(0, 0),
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
        match self.runtime.block_on(
            self.reader
                .read_at(&node.path, offset as u64, size as usize),
        ) {
            Ok(bytes) => reply.data(&bytes),
            Err(_) => reply.error(libc::EIO),
        }
    }
}
