use protostore_core::{
    BlobStore, ChunkId, FileChunkRef, FileNode, Hash32, LocalCache, ObjectBlobStore, PackConfig,
    ProfileRecorder, TreeId, TreeReader,
    pack::{FOOTER_LEN, compress_chunk, decompress_chunk, encode_pack, parse_footer},
    pack_directory_with_config,
    profile::write_profile,
    tree::{LogicalTreeManifest, load_layout, load_tree, pack_directory, repack_tree, tree_id},
};
use std::{fs, path::Path};
use tempfile::TempDir;

fn write(path: &Path, bytes: &[u8]) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, bytes).unwrap();
}

fn local_store(dir: &TempDir) -> ObjectBlobStore {
    ObjectBlobStore::from_uri(&format!("file://{}", dir.path().display())).unwrap()
}

#[test]
fn pack_footer_roundtrip() {
    let chunk = b"hello pack".to_vec();
    let chunk_id = ChunkId::new(Hash32::digest(&chunk));
    let compressed = compress_chunk(&chunk).unwrap();
    let encoded = encode_pack(&[(chunk_id, compressed, chunk.len() as u32)]).unwrap();
    let footer = &encoded.bytes[encoded.bytes.len() - FOOTER_LEN..];
    let (offset, len, hash) = parse_footer(footer).unwrap();
    let index_bytes = &encoded.bytes[offset as usize..(offset + len) as usize];
    assert_eq!(Hash32::digest(index_bytes), hash);
    assert_eq!(encoded.index.chunks[0].chunk_id, chunk_id);
}

#[test]
fn chunk_compression_roundtrip() {
    let bytes = b"content that survives compression".repeat(1024);
    let compressed = compress_chunk(&bytes).unwrap();
    assert_eq!(decompress_chunk(&compressed).unwrap(), bytes);
}

#[test]
fn tree_manifest_deterministic_hash() {
    let chunk_id = ChunkId::new(Hash32::digest(b"a"));
    let tree = LogicalTreeManifest {
        version: 1,
        files: vec![FileNode {
            path: "a.txt".into(),
            mode: 0o644,
            size: 1,
            chunks: vec![FileChunkRef {
                file_offset: 0,
                chunk_offset: 0,
                uncompressed_len: 1,
                chunk_id,
            }],
        }],
    };
    assert_eq!(tree_id(&tree).unwrap(), tree_id(&tree.clone()).unwrap());
}

#[tokio::test]
async fn range_read_single_chunk() {
    let input = TempDir::new().unwrap();
    let store_dir = TempDir::new().unwrap();
    let cache_dir = TempDir::new().unwrap();
    write(&input.path().join("one.txt"), b"abcdef");
    let store = local_store(&store_dir);
    let tree_id = pack_directory(&store, input.path()).await.unwrap();
    let reader = TreeReader::open(store, tree_id, LocalCache::new(cache_dir.path()))
        .await
        .unwrap();
    assert_eq!(reader.read_at("one.txt", 2, 3).await.unwrap(), "cde");
}

#[tokio::test]
async fn range_read_spanning_multiple_chunks() {
    let input = TempDir::new().unwrap();
    let store_dir = TempDir::new().unwrap();
    let cache_dir = TempDir::new().unwrap();
    let mut bytes = vec![b'a'; 4 * 1024 * 1024];
    bytes.extend_from_slice(b"boundary");
    write(&input.path().join("large.bin"), &bytes);
    let store = local_store(&store_dir);
    let tree_id = pack_directory_with_config(
        &store,
        input.path(),
        PackConfig {
            chunk_size: 4 * 1024 * 1024,
            pack_workers: 2,
            pack_key_prefix: "test-large".to_string(),
        },
    )
    .await
    .unwrap();
    let reader = TreeReader::open(store, tree_id, LocalCache::new(cache_dir.path()))
        .await
        .unwrap();
    let got = reader
        .read_at("large.bin", (4 * 1024 * 1024 - 3) as u64, 10)
        .await
        .unwrap();
    assert_eq!(&got[..], b"aaaboundar");
}

#[tokio::test]
async fn pack_directory_and_materialize_roundtrip() {
    let input = TempDir::new().unwrap();
    let output = TempDir::new().unwrap();
    let store_dir = TempDir::new().unwrap();
    let cache_dir = TempDir::new().unwrap();
    write(&input.path().join("nested/a.txt"), b"alpha");
    write(&input.path().join("b.txt"), b"beta");
    let store = local_store(&store_dir);
    let tree_id = pack_directory(&store, input.path()).await.unwrap();
    let reader = TreeReader::open(store, tree_id, LocalCache::new(cache_dir.path()))
        .await
        .unwrap();
    reader.materialize(output.path()).await.unwrap();
    assert_eq!(
        fs::read(output.path().join("nested/a.txt")).unwrap(),
        b"alpha"
    );
    assert_eq!(fs::read(output.path().join("b.txt")).unwrap(), b"beta");
}

#[tokio::test]
async fn pack_directory_bundles_small_files_into_shared_chunks() {
    let input = TempDir::new().unwrap();
    let store_dir = TempDir::new().unwrap();
    let cache_dir = TempDir::new().unwrap();
    write(&input.path().join("a.txt"), b"alpha");
    write(&input.path().join("b.txt"), b"beta");
    let store = local_store(&store_dir);
    let tree_id = pack_directory_with_config(
        &store,
        input.path(),
        PackConfig {
            chunk_size: 1024,
            pack_workers: 2,
            pack_key_prefix: "test-bundle".to_string(),
        },
    )
    .await
    .unwrap();
    let tree = load_tree(&store, tree_id).await.unwrap();
    let layout = load_layout(&store, tree.layout_id).await.unwrap();
    assert_eq!(layout.locations.len(), 1);
    assert_eq!(tree.files.len(), 2);
    assert_eq!(
        tree.files[0].chunks[0].chunk_id,
        tree.files[1].chunks[0].chunk_id
    );
    assert_eq!(tree.files[0].chunks[0].chunk_offset, 0);
    assert_eq!(tree.files[1].chunks[0].chunk_offset, 5);

    let reader = TreeReader::open(store, tree_id, LocalCache::new(cache_dir.path()))
        .await
        .unwrap();
    assert_eq!(reader.read_at("a.txt", 1, 3).await.unwrap(), "lph");
    assert_eq!(reader.read_at("b.txt", 1, 2).await.unwrap(), "et");
}

#[tokio::test]
async fn tree_reader_full_and_partial_file_reads() {
    let input = TempDir::new().unwrap();
    let store_dir = TempDir::new().unwrap();
    let cache_dir = TempDir::new().unwrap();
    write(&input.path().join("file.txt"), b"0123456789");
    let store = local_store(&store_dir);
    let id = pack_directory(&store, input.path()).await.unwrap();
    let reader = TreeReader::open(store, id, LocalCache::new(cache_dir.path()))
        .await
        .unwrap();
    assert_eq!(
        reader.read_at("file.txt", 0, 10).await.unwrap(),
        "0123456789"
    );
    assert_eq!(reader.read_at("file.txt", 4, 3).await.unwrap(), "456");
}

#[test]
fn access_profile_aggregation() {
    let recorder = ProfileRecorder::default();
    let tree_id = TreeId::new(Hash32::digest(b"tree"));
    let chunk_id = ChunkId::new(Hash32::digest(b"chunk"));
    recorder.record(tree_id, chunk_id);
    recorder.record(tree_id, chunk_id);
    let profile = recorder.profile();
    assert_eq!(profile.events.len(), 1);
    assert_eq!(profile.events[0].read_count, 2);
}

#[tokio::test]
async fn repack_produces_readable_output() {
    let input = TempDir::new().unwrap();
    let store_dir = TempDir::new().unwrap();
    let cache_dir = TempDir::new().unwrap();
    write(&input.path().join("a.txt"), b"alpha");
    write(&input.path().join("b.txt"), b"beta");
    let store = local_store(&store_dir);
    let original_tree = pack_directory(&store, input.path()).await.unwrap();
    let reader = TreeReader::open(
        store.clone(),
        original_tree,
        LocalCache::new(cache_dir.path()),
    )
    .await
    .unwrap()
    .with_recorder(ProfileRecorder::default());
    assert_eq!(reader.read_at("b.txt", 0, 4).await.unwrap(), "beta");
    assert_eq!(reader.read_at("a.txt", 0, 5).await.unwrap(), "alpha");
    let profile_id = write_profile(&store, &reader.recorder().unwrap().profile())
        .await
        .unwrap();
    let original_layout = load_tree(&store, original_tree).await.unwrap().layout_id;
    let new_tree = repack_tree(&store, original_tree, profile_id)
        .await
        .unwrap();
    assert_eq!(new_tree, original_tree);
    let updated_layout = load_tree(&store, original_tree).await.unwrap().layout_id;
    assert_ne!(updated_layout, original_layout);
    let new_reader = TreeReader::open(store, new_tree, LocalCache::new(cache_dir.path()))
        .await
        .unwrap();
    assert_eq!(new_reader.read_at("a.txt", 0, 5).await.unwrap(), "alpha");
    assert_eq!(new_reader.read_at("b.txt", 0, 4).await.unwrap(), "beta");
}

#[tokio::test]
async fn tree_object_is_written_last_and_loadable() {
    let input = TempDir::new().unwrap();
    let store_dir = TempDir::new().unwrap();
    write(&input.path().join("x"), b"x");
    let store = local_store(&store_dir);
    let id = pack_directory(&store, input.path()).await.unwrap();
    let tree = load_tree(&store, id).await.unwrap();
    let layout = load_layout(&store, tree.layout_id).await.unwrap();
    assert_eq!(tree.files.len(), 1);
    assert_eq!(layout.tree_id, id);
    assert_eq!(layout.locations.len(), 1);
    assert!(store.exists(&format!("trees/{id}.tree")).await.unwrap());
    assert!(
        store
            .exists(&format!("layouts/{}.layout", tree.layout_id))
            .await
            .unwrap()
    );
}
