# data-processor v0.1.0

Released: 2026-10-05

Streaming projections and history. First coordinated OpenRec source release.

## Features

- A shared feature-core and alternative Spark Structured Streaming and Flink implementations.
- Kafka user/item/event mutation consumption, Redis serving projections, HBase raw archives and UTC-partitioned Hive/HDFS history.
- Mutation identity, duplicate handling, tombstones, rolling behavioral features and event-time semantics.
- Action/conversion and commerce statistics with shared online/offline feature definitions.
- Golden parity fixtures, Flink state harness tests, Spark micro-batch tests and versioned JSON feature-state recovery.

## Installation and compatibility

Requires Java 21, Spark 4.0.4/Scala 2.13 or Flink 2.2.1, and rec-proto `0.1.0`. Engine jars and feature-core are versioned `0.1.0`.

Deploy one streaming implementation for a production projection. Append-oriented history can contain retry duplicates, so offline readers must resolve identities and tombstones. Preserve checkpoints and full history for upgrades; Flink 1.x savepoint compatibility is not guaranteed. Additional Redis window statistics require replay/backfill for inactive entities.

## Validation and known boundaries

See this repository's README for build/test commands and deployment requirements. The coordinated release's [validation record](https://github.com/open-rec/openrec/blob/v0.1.0/release/VALIDATION.md) distinguishes checks executed for this release from historical integration evidence.

This initial release establishes a versioned source baseline. Source archives and checksums are published; external package registries and container registries are not populated by the source-release workflow. Upgrade the complete compatible distribution, retain data/checkpoints/artifacts, and preserve prior component refs for rollback.
