# protostore

`protostore` is an object-store-backed, content-addressed, immutable tree store with range-readable compressed packs and lazy FUSE mounting.

It is the storage substrate for a future `protoci`: `protostore` owns packs, chunks, tree manifests, lazy reads, FUSE mounts, access profiles, and repacking. `protoci` should later build CI actions, checkpoints, executors, and pipeline orchestration on top of `TreeId`s without duplicating storage logic.

## MVP Support

- Pack a directory into zstd-compressed, independently decompressible chunks.
- Store immutable pack blobs, tree manifests, and access profiles in object storage.
- Use `file://` stores through `object_store::local::LocalFileSystem`.
- Read files lazily through `TreeReader::read_at`.
- Materialize a stored tree back to a normal directory.
- Inspect tree metadata.
- Mount a tree read-only through FUSE.
- Repack chunks in access-profile order.

## Not Supported Yet

No CI executor, action keys, checkpoints, writable overlays, GC, inline small-file optimization, leases, distributed scheduling, advanced permissions, or remote precondition support are implemented in this MVP.

## Local Quickstart

```bash
cargo run -p protostore-cli -- pack ./examples/data --store file:///tmp/protostore-store
cargo run -p protostore-cli -- inspect <tree-id> --store file:///tmp/protostore-store
cargo run -p protostore-cli -- materialize <tree-id> /tmp/protostore-out --store file:///tmp/protostore-store
```

For FUSE:

```bash
mkdir -p /tmp/protostore-mnt
cargo run -p protostore-cli -- mount <tree-id> /tmp/protostore-mnt --store file:///tmp/protostore-store
cat /tmp/protostore-mnt/some-file
```

## Storage Layout

All durable state lives under object-store keys:

```text
packs/<pack-id>.pack
trees/<tree-id>.tree
profiles/<profile-id>.profile
refs/<name>.ref
```

Local disk is disposable cache only, currently `.protostore-cache/chunks/<chunk-id>`.

## Pack Format

Pack blobs use a fixed header, independently compressed zstd chunk frames, a JSON index, and a fixed footer. Readers parse the footer, verify the index hash, resolve chunk offsets, then range-read only the compressed chunk frames needed for a file read.

Implementation note: the original MVP spec put `pack_id` inside the hashed pack index while also defining `pack_id` as the hash of the full pack bytes. This implementation skips serializing `pack_id` inside the index and fills it from the pack key when reading.

## Lazy Reads And FUSE

`TreeReader` maps logical file ranges to chunk references, resolves chunk location hints, reads compressed pack ranges from object storage, decompresses only required chunks, and stores decompressed chunks in the local disposable cache. The FUSE crate is intentionally thin and delegates file content reads to `TreeReader`.

## Known Limitations

- `file://` stores are implemented first; S3/GCS/Azure URI parsing is not wired yet.
- Pack index loading currently fetches the full pack before parsing the footer, while file-content reads use exact ranges.
- Access profiles are implemented in core, but the CLI does not yet expose explicit profiling flags for materialize/read.
- Embedded location hints mean repacking creates a new tree ID in the MVP.
