# OpenRec Real-Time Data Processor

[![CI](https://github.com/open-rec/data-processor/actions/workflows/ci.yml/badge.svg)](https://github.com/open-rec/data-processor/actions/workflows/ci.yml)
![Java](https://img.shields.io/badge/Java-8-ED8B00?logo=openjdk&logoColor=white)
![Flink](https://img.shields.io/badge/Flink-1.14.6-E6526F?logo=apacheflink&logoColor=white)
![Spark](https://img.shields.io/badge/Spark-3.5.3-E25A1C?logo=apachespark&logoColor=white)

`data-processor` provides equivalent Flink and Spark Structured Streaming jobs. Both consume the `user`, `item`, and `event` Kafka topics, update Redis serving data, persist the original entities in HBase, and append immutable JSON records to Hive-backed HDFS locations for offline training.

## Feature Contract

Feature formulas live in `feature-core`; Flink and Spark only supply engine-specific state and sinks. Raw user fields (`id`, device/profile/location/tags and register/login time) and item fields (`id`, title/category/tags/scene, lifecycle/status and weight) are retained unchanged. Event streams generate the same behavioral columns as `rec-algorithm/algorithm/feature/event_feature.py`:

- totals: `event_count`, value sum/mean, active days, unique scenes and counterpart count;
- time: first/last event, recency, and 1/7/30-day counts;
- actions: click, expose, buy, collect and stay counts, plus click rate.

Each event updates both its user and item snapshot. Redis keys are `feature:user:{id}` and `feature:item:{id}`. Raw serving keys remain compatible with rec-server (`user:{id}`, `item:{id}`, `event:{userId}:scene:type`, and `new:{scene}`). A structured `dislike` value is expanded into `id:`, `category:`, and one or more `tag:` members so BlackNode can apply it online; other event members remain item IDs. `new:{scene}` is a sorted set scored by `pubTime`; `redis.new.max-items` bounds every scene so the realtime projection cannot grow without limit.

## Durable Training Data

Both jobs preserve the Kafka JSON byte-for-byte in HBase tables `openrec_user`, `openrec_item`, and `openrec_event`, under column `entity:json`. User and item ids are row keys; events use `traceId`, falling back to `time#userId#itemId#scene#type`. Tables are created idempotently when a task starts.

The same payloads are appended under
`hdfs://namenode:8020/openrec/hive/{user,item,event}/dt=YYYY-MM-DD`. The UTC partition date comes
from event `time`, item `modifyTime`/`pubTime`, or user `loginTime`/`registerTime`; malformed or
missing timestamps fall back to the processing date. Install the partitioned external Hive tables
once after the cluster starts:

```bash
docker exec -i hiveserver2 /opt/hive/bin/beeline \
  -u jdbc:hive2://hiveserver2:10000 -n hive \
  -f /opt/workspace/data-processor/sql/openrec_entities.sql
```

If the source tree is not mounted at `/opt/workspace`, copy the SQL file into the container first. Offline embedding, i2i, and hot training jobs should read `openrec.user_entity`, `openrec.item_entity`, and `openrec.event_entity`, parse the JSON fields they require, and publish serving outputs to Redis/Elasticsearch. Feature snapshots remain under `/openrec/features`. Checkpoints are stored separately; never use checkpoint files as training input.

The DDL changed from an unpartitioned table to `PARTITIONED BY (dt STRING)`. Drop and recreate an
older development table before deploying this version (external data is not deleted). Scheduled
algorithm jobs register only their requested day with `ALTER TABLE ADD IF NOT EXISTS PARTITION`.

Kafka messages use the version 1 mutation envelope published by `rec-server`: entity type,
`INSERT`/`UPDATE`/`DELETE` operation, event time, and the entity payload. Both processors also accept
legacy bare-entity JSON as `INSERT` during the compatibility window. `INSERT` and `UPDATE` are
upserts. User and item deletes remove Redis serving state and append tombstones to historical
storage so cumulative offline readers do not resurrect deleted entities. Event deletion is not
currently accepted by `rec-server`; event history remains append-only. See
[`rec-proto`](https://github.com/open-rec/rec-server/tree/master/proto) for the shared mutation
contract.

## Build and Run

Use JDK 8:

```bash
mvn clean test
mvn -pl flink -am -DskipTests package
mvn -pl spark -am -DskipTests package
```

Submit one implementation for production, using the properties bundled in its jar:

```bash
docker cp flink/target/rec-flink-1.0-SNAPSHOT.jar \
  flink-jobmanager:/opt/flink/jobs/openrec-features.jar
docker exec flink-jobmanager flink run -d -c com.openrec.dp.flink.DpJob \
  /opt/flink/jobs/openrec-features.jar
spark-submit --class com.openrec.dp.spark.SparkFeatureJob \
  --master spark://spark-master:7077 spark/target/rec-spark-1.0-SNAPSHOT.jar
```

Configure Kafka, Redis, HBase, Hive/HDFS, checkpoint paths, parallelism, and event lateness in each module's `src/main/resources/dp.properties`. Set `hbase.enabled=false` or `hive.enabled=false` only when intentionally running without that cluster component. Use distinct Kafka consumer groups and checkpoint directories when comparing engines. Running both against the same topics duplicates persisted entities, although stable HBase row keys make user/item updates idempotent.

## Delivery and compatibility

The two engines implement the same projection contract but are alternative deployments, not an
active/active pair. Their default consumer groups are distinct, so starting both processes every
message twice. Redis user/item writes and HBase user/item row keys are idempotent; event history and
Hive/HDFS output are append-oriented and can contain duplicates after retries or dual-engine runs.
Offline readers must collapse mutations by entity id and event time and apply the latest DELETE
tombstone.

Flink checkpoints every 60 seconds and allows 30 seconds of event-time disorder by default. Spark
uses the HDFS checkpoint path configured in `dp.properties`. Preserve or deliberately migrate these
locations during an engine upgrade; checkpoint files are recovery state, not training input.

This repository currently builds Java 8 bytecode against Flink 1.14.6, Spark 3.5.3/Scala 2.12,
HBase 2.5.10, and the Kafka endpoints supplied by `bigdata-platform`. Compile-time success on a newer
JDK does not establish cluster-runtime compatibility; upgrade the corresponding platform image and
run the cluster deletion, persistence, and recall acceptance flows together.

## Testing

`feature-core` unit tests validate user/item aggregation, rolling windows, action counts, and click-rate semantics. When changing a feature in `rec-algorithm`, update the shared contract and its tests in the same change so online and offline definitions stay aligned.

`mvn clean test` is the local unit boundary. Redis, Kafka, HBase, Hive, checkpoint recovery, and
DELETE tombstones are verified by the distribution-level cluster acceptance flow in
[`example`](https://github.com/open-rec/example); do not treat one engine's unit suite as complete
contract validation.
