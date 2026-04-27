# protostore

`protostore` is an object-store-backed, content-addressed, immutable tree store with range-readable compressed packs and lazy FUSE mounting.

It is the storage substrate for a future `protoci`: `protostore` owns packs, chunks, tree manifests, lazy reads, FUSE mounts, access profiles, and repacking. `protoci` should later build CI actions, checkpoints, executors, and pipeline orchestration on top of keyed immutable trees without duplicating storage logic.

## MVP Support

- Pack a directory into zstd-compressed, independently decompressible chunks.
- Store immutable pack blobs, tree manifests, and access profiles in object storage.
- Use `file://` stores through `object_store::local::LocalFileSystem`.
- Use `gs://bucket/prefix` stores through the `object_store` GCS adapter.
- Read files lazily through `TreeReader::read_at`.
- Materialize a stored tree back to a normal directory.
- Inspect tree metadata.
- Mount a tree read-only through FUSE.
- Repack chunks in access-profile order.

## Not Supported Yet

No CI executor, action keys, checkpoints, writable overlays, GC, inline small-file optimization, leases, distributed scheduling, advanced permissions, or remote precondition support are implemented in this MVP.

## Local Quickstart

```bash
cargo run -p protostore-cli -- pack ./examples/data --store file:///tmp/protostore-store --chunk-size 16MiB --compression-level 0 --pack-workers 8
cargo run -p protostore-cli -- inspect <key> --store file:///tmp/protostore-store
cargo run -p protostore-cli -- materialize <key> /tmp/protostore-out --store file:///tmp/protostore-store --target-coalesce 64MiB
```

For FUSE:

```bash
mkdir -p /tmp/protostore-mnt
cargo run -p protostore-cli -- mount <key> /tmp/protostore-mnt --store file:///tmp/protostore-store
cat /tmp/protostore-mnt/some-file
```

Useful size flags accept plain bytes or `KiB`/`MiB`/`GiB` suffixes:

```bash
protostore pack <dir> --store file:///tmp/store --chunk-size 16MiB --compression-level 3 --pack-workers 8 --key cargo-cache/<cache-key>
protostore mount <key> <mnt> --store file:///tmp/store --min-remote-read 16MiB --target-coalesce 64MiB --read-ahead-chunks 4 --read-ahead-bytes 64MiB --read-ahead-concurrency 2
protostore materialize <key> <out> --store file:///tmp/store --min-remote-read 16MiB --target-coalesce 64MiB --read-ahead-chunks 4 --read-ahead-bytes 64MiB --read-ahead-concurrency 2
```

For GCS, authenticate through the environment variables supported by `object_store`:

```bash
export SERVICE_ACCOUNT=/path/to/service-account.json
# or:
export GOOGLE_SERVICE_ACCOUNT=/path/to/service-account.json
# or ADC:
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/application-default-credentials.json

protostore pack ./examples/data --store gs://my-bucket/protostore --key examples/data
protostore inspect examples/data --store gs://my-bucket/protostore
protostore materialize examples/data /tmp/protostore-out --store gs://my-bucket/protostore
```

## Storage Layout

All durable state lives under object-store keys:

```text
trees/<key>.tree
packs/<key>.pack
profiles/<profile-id>.profile
refs/<name>.ref
```

Local disk is disposable cache only, currently `.protostore-cache/chunks/<chunk-id>`.

## Pack Format

Pack blobs use a fixed header, independently compressed zstd chunk frames, a JSON index, and a fixed footer. Tree manifests describe logical files, chunk IDs, and the physical pack offsets for those chunks. Readers resolve chunk locations through the tree, then range-read only the compressed chunk frames needed for a file read.

Pack and tree objects are written under the same key. `--key` controls the object names below `packs/` and `trees/`; if omitted, the CLI uses a generated UUIDv7 key and prints it. Each pack operation currently writes `packs/<key>.pack` and `trees/<key>.tree`. Manifests still store `tree_id` and `pack_hash` for integrity instead of requiring content-addressed object names.

Packing uses bounded `tokio-uring` workers on Linux to read files and compute chunk hashes/compression concurrently. The CLI defaults `--pack-workers` to the number of available CPU threads and `--compression-level` to `0`, which uses zstd's default level. Tree identity is based on logical file content.

For `file://` stores, pack uploads write directly to the final local path so streaming progress is visible on disk. Remote stores use the provider multipart writer exposed by `object_store`; the object should be treated as committed only after the upload finishes.

## Lazy Reads And FUSE

`TreeReader` loads a tree, maps logical file ranges to chunk references, resolves chunk locations, coalesces adjacent compressed chunk reads from the same pack, decompresses selected chunks, and stores decompressed chunks in the local disposable cache. After a read, it schedules bounded read-ahead for following chunks so sequential FUSE reads can overlap future object-store range requests. The FUSE crate is intentionally thin and delegates file content reads to `TreeReader`.

Enable tracing to inspect lazy reads:

```bash
rm -rf .protostore-cache
RUST_LOG='protostore::reader=debug,protostore::object_store=debug' \
  protostore mount <key> <mnt> --store file:///tmp/store
```

Look for `read_at selected chunk`, `start read-ahead`, `fetch coalesced compressed chunk range`, `object range read`, `decompress chunk`, and `chunk cache hit`.

## Known Limitations

- `gs://` is wired through `object_store`, but GCS integration tests require real credentials and are not part of the default test suite.
- Pack index loading currently fetches the full pack before parsing the footer, while file-content reads use coalesced compressed chunk ranges.
- Access profiles are implemented in core, but the CLI does not yet expose explicit profiling flags for materialize/read.
