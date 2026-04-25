use crate::{
    hash::{ChunkId, Hash32, PackId},
    store::BlobStore,
};
use anyhow::{Context, Result, bail, ensure};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const PACK_MAGIC: &[u8; 8] = b"PSTPACK\0";
pub const FOOTER_MAGIC: &[u8; 8] = b"PSTEND\0\0";
pub const PACK_VERSION: u32 = 1;
pub const HEADER_LEN: usize = 16;
pub const FOOTER_LEN: usize = 56;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PackIndex {
    pub version: u32,
    #[serde(skip_serializing, default)]
    pub pack_id: PackId,
    pub chunks: Vec<ChunkEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkEntry {
    pub chunk_id: ChunkId,
    pub compressed_offset: u64,
    pub compressed_len: u32,
    pub uncompressed_len: u32,
    pub compression: Compression,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Compression {
    Zstd,
}

#[derive(Debug, Clone)]
pub struct EncodedPack {
    pub pack_id: PackId,
    pub bytes: Bytes,
    pub index: PackIndex,
}

pub fn compress_chunk(bytes: &[u8]) -> Result<Vec<u8>> {
    zstd::stream::encode_all(bytes, 0).context("zstd compress chunk")
}

pub fn decompress_chunk(bytes: &[u8]) -> Result<Vec<u8>> {
    zstd::stream::decode_all(bytes).context("zstd decompress chunk")
}

pub fn encode_pack(chunks: &[(ChunkId, Vec<u8>, u32)]) -> Result<EncodedPack> {
    let mut out = BytesMut::new();
    out.extend_from_slice(PACK_MAGIC);
    out.put_u32_le(PACK_VERSION);
    out.put_u32_le(0);

    let mut entries = Vec::with_capacity(chunks.len());
    for (chunk_id, compressed, uncompressed_len) in chunks {
        let compressed_offset = u64::try_from(out.len()).context("pack offset overflow")?;
        out.extend_from_slice(compressed);
        entries.push(ChunkEntry {
            chunk_id: *chunk_id,
            compressed_offset,
            compressed_len: u32::try_from(compressed.len())
                .context("compressed chunk too large")?,
            uncompressed_len: *uncompressed_len,
            compression: Compression::Zstd,
        });
    }

    let index_offset = u64::try_from(out.len()).context("index offset overflow")?;
    let index = PackIndex {
        version: PACK_VERSION,
        pack_id: PackId::zero(),
        chunks: entries,
    };
    let index_bytes = serde_json::to_vec(&index).context("serialize pack index")?;
    let index_hash = Hash32::digest(&index_bytes);
    out.extend_from_slice(&index_bytes);
    out.put_u64_le(index_offset);
    out.put_u64_le(u64::try_from(index_bytes.len()).context("index length overflow")?);
    out.extend_from_slice(&index_hash.0);
    out.extend_from_slice(FOOTER_MAGIC);

    let bytes = out.freeze();
    let pack_id = PackId::new(Hash32::digest(&bytes));
    let mut index = read_pack_index_from_bytes(&bytes)?;
    index.pack_id = pack_id;
    Ok(EncodedPack {
        pack_id,
        bytes,
        index,
    })
}

pub fn read_pack_index_from_bytes(bytes: &[u8]) -> Result<PackIndex> {
    ensure!(bytes.len() >= HEADER_LEN + FOOTER_LEN, "pack too small");
    ensure!(&bytes[..8] == PACK_MAGIC, "invalid pack header magic");
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    ensure!(
        version == PACK_VERSION,
        "unsupported pack version {version}"
    );
    let footer = &bytes[bytes.len() - FOOTER_LEN..];
    ensure!(&footer[48..56] == FOOTER_MAGIC, "invalid pack footer magic");
    let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
    let index_len = u64::from_le_bytes(footer[8..16].try_into().unwrap());
    let mut expected_hash = [0; 32];
    expected_hash.copy_from_slice(&footer[16..48]);
    let start = usize::try_from(index_offset).context("index offset overflow")?;
    let end = start
        .checked_add(usize::try_from(index_len).context("index length overflow")?)
        .context("index range overflow")?;
    ensure!(end <= bytes.len() - FOOTER_LEN, "index points outside pack");
    let index_bytes = &bytes[start..end];
    ensure!(
        Hash32::digest(index_bytes).0 == expected_hash,
        "pack index hash mismatch"
    );
    serde_json::from_slice(index_bytes).context("deserialize pack index")
}

pub async fn read_pack_index<S: BlobStore>(store: &S, pack_id: PackId) -> Result<PackIndex> {
    let key = pack_key(pack_id);
    let pack = store.get_bytes(&key).await?;
    let start = pack
        .len()
        .checked_sub(FOOTER_LEN)
        .context("pack too small")?;
    let footer = pack.slice(start..);
    ensure!(footer.len() == FOOTER_LEN, "short pack footer");
    ensure!(&footer[48..56] == FOOTER_MAGIC, "invalid pack footer magic");
    let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
    let index_len = u64::from_le_bytes(footer[8..16].try_into().unwrap());
    let mut expected_hash = [0; 32];
    expected_hash.copy_from_slice(&footer[16..48]);
    let index_bytes = store.get_range(&key, index_offset, index_len).await?;
    ensure!(
        Hash32::digest(&index_bytes).0 == expected_hash,
        "pack index hash mismatch"
    );
    let mut index: PackIndex =
        serde_json::from_slice(&index_bytes).context("deserialize pack index")?;
    index.pack_id = pack_id;
    Ok(index)
}

pub async fn read_chunk_from_pack<S: BlobStore>(
    store: &S,
    pack_id: PackId,
    entry: &ChunkEntry,
) -> Result<Bytes> {
    let compressed = store
        .get_range(
            &pack_key(pack_id),
            entry.compressed_offset,
            u64::from(entry.compressed_len),
        )
        .await?;
    let decompressed = decompress_chunk(&compressed)?;
    ensure!(
        decompressed.len() == entry.uncompressed_len as usize,
        "decompressed chunk length mismatch"
    );
    Ok(Bytes::from(decompressed))
}

pub fn index_by_chunk(index: &PackIndex) -> HashMap<ChunkId, ChunkEntry> {
    index
        .chunks
        .iter()
        .map(|entry| (entry.chunk_id, entry.clone()))
        .collect()
}

pub fn pack_key(pack_id: PackId) -> String {
    format!("packs/{pack_id}.pack")
}

pub fn parse_footer(bytes: &[u8]) -> Result<(u64, u64, Hash32)> {
    if bytes.len() != FOOTER_LEN {
        bail!("footer must be {FOOTER_LEN} bytes");
    }
    ensure!(&bytes[48..56] == FOOTER_MAGIC, "invalid pack footer magic");
    let index_offset = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let index_len = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let mut hash = [0; 32];
    hash.copy_from_slice(&bytes[16..48]);
    Ok((index_offset, index_len, Hash32(hash)))
}
