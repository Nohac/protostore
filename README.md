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
cargo run -p protostore-cli -- pack ./examples/data --store file:///tmp/protostore-store --chunk-size 16MiB --pack-workers 8
cargo run -p protostore-cli -- inspect <tree-id> --store file:///tmp/protostore-store
cargo run -p protostore-cli -- materialize <tree-id> /tmp/protostore-out --store file:///tmp/protostore-store --target-coalesce 64MiB
```

For FUSE:

```bash
mkdir -p /tmp/protostore-mnt
cargo run -p protostore-cli -- mount <tree-id> /tmp/protostore-mnt --store file:///tmp/protostore-store
cat /tmp/protostore-mnt/some-file
```

Useful size flags accept plain bytes or `KiB`/`MiB`/`GiB` suffixes:

```bash
protostore pack <dir> --store file:///tmp/store --chunk-size 16MiB --pack-target-size 128MiB --pack-workers 8
protostore mount <tree-id> <mnt> --store file:///tmp/store --min-remote-read 16MiB --target-coalesce 64MiB
protostore materialize <tree-id> <out> --store file:///tmp/store --min-remote-read 16MiB --target-coalesce 64MiB
```

## Storage Layout

All durable state lives under object-store keys:

```text
packs/<pack-id>.pack
trees/<tree-id>.tree
layouts/<layout-id>.layout
profiles/<profile-id>.profile
refs/<name>.ref
```

Local disk is disposable cache only, currently `.protostore-cache/chunks/<chunk-id>`.

## Pack Format

Pack blobs use a fixed header, independently compressed zstd chunk frames, a JSON index, and a fixed footer. Tree manifests describe logical files and chunk IDs. Layout manifests map chunk IDs to physical pack offsets. Readers parse the footer, verify the index hash, resolve chunk offsets through the layout, then range-read only the compressed chunk frames needed for a file read.

Implementation note: the original MVP spec put `pack_id` inside the hashed pack index while also defining `pack_id` as the hash of the full pack bytes. This implementation skips serializing `pack_id` inside the index and fills it from the pack key when reading.

Packing uses bounded `tokio-uring` workers on Linux to read files and compute chunk hashes/compression concurrently. The CLI defaults `--pack-workers` to the number of available CPU threads. Tree identity is based on logical file content; physical pack layout is stored separately in a layout object.

## Lazy Reads And FUSE

`TreeReader` loads a tree and its referenced layout, maps logical file ranges to chunk references, resolves chunk locations, coalesces adjacent compressed chunk reads from the same pack, decompresses selected chunks, and stores decompressed chunks in the local disposable cache. The FUSE crate is intentionally thin and delegates file content reads to `TreeReader`.

Enable tracing to inspect lazy reads:

```bash
rm -rf .protostore-cache
RUST_LOG='protostore::reader=debug,protostore::object_store=debug' \
  protostore mount <tree-id> <mnt> --store file:///tmp/store
```

Look for `read_at selected chunk`, `fetch coalesced compressed chunk range`, `object range read`, `decompress chunk`, and `chunk cache hit`.

## Known Limitations

- `file://` stores are implemented first; S3/GCS/Azure URI parsing is not wired yet.
- Pack index loading currently fetches the full pack before parsing the footer, while file-content reads use coalesced compressed chunk ranges.
- Access profiles are implemented in core, but the CLI does not yet expose explicit profiling flags for materialize/read.
