# Real-time Feature Processor

`data-processor` provides equivalent Flink and Spark Structured Streaming jobs. Both consume the `user`, `item`, and `event` Kafka topics, update Redis serving data, persist the original entities in HBase, and append immutable JSON records to Hive-backed HDFS locations for offline training.

## Feature Contract

Feature formulas live in `feature-core`; Flink and Spark only supply engine-specific state and sinks. Raw user fields (`id`, device/profile/location/tags and register/login time) and item fields (`id`, title/category/tags/scene, lifecycle/status and weight) are retained unchanged. Event streams generate the same behavioral columns as `rec-algorithm/algorithm/feature/event_feature.py`:

- totals: `event_count`, value sum/mean, active days, unique scenes and counterpart count;
- time: first/last event, recency, and 1/7/30-day counts;
- actions: click, expose, buy, collect and stay counts, plus click rate.

Each event updates both its user and item snapshot. Redis keys are `feature:user:{id}` and `feature:item:{id}`. Raw serving keys remain compatible with rec-server (`user:{id}`, `item:{id}`, `event:{userId}:scene:type`, and `new:{scene}`). `new:{scene}` is a sorted set scored by `pubTime`; `redis.new.max-items` bounds every scene so the realtime projection cannot grow without limit.

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

Kafka payloads currently contain records rather than command envelopes. Consequently, the processors support inserts/upserts; delete semantics require rec-server to publish `PushCmd` in a future schema version.

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

## Testing

`feature-core` unit tests validate user/item aggregation, rolling windows, action counts, and click-rate semantics. When changing a feature in `rec-algorithm`, update the shared contract and its tests in the same change so online and offline definitions stay aligned.
