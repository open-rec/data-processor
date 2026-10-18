# data-processor v0.2.0

Released: 2026-10-18

Part of the coordinated OpenRec v0.2.0 source release.

## Changes since v0.1.0

- Refactor Flink and Spark jobs into source, process and sink layers.
- Preserve versioned mutation, online feature and offline history contracts across both engines.

## Installation and compatibility

Requires Java 21 and rec-proto 0.2.0. Flink and Spark artifacts are versioned 0.2.0. Preserve Kafka ordering keys, tombstones, UTC partitions and existing checkpoints during upgrades.

See the [distribution upgrade and rollback instructions](https://github.com/open-rec/openrec/blob/v0.2.0/RELEASE_NOTES.md) for the complete compatible component set and persisted configuration migrations.

## Validation and publication

The [validation record](https://github.com/open-rec/openrec/blob/v0.2.0/release/VALIDATION.md) distinguishes release checks from maintainer-reported prior CI and integration results. GitHub Actions publishes source archives and SHA-256 checksums; Maven Central, PyPI and versioned container registries are not populated by this workflow.
