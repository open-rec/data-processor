# Real-time Feature Processor

`data-processor` provides equivalent Flink and Spark Structured Streaming jobs. Both consume the `user`, `item`, and `event` Kafka topics, update Redis serving data, and append immutable JSON records to HDFS for offline training.

## Feature Contract

Feature formulas live in `feature-core`; Flink and Spark only supply engine-specific state and sinks. Raw user fields (`id`, device/profile/location/tags and register/login time) and item fields (`id`, title/category/tags/scene, lifecycle/status and weight) are retained unchanged. Event streams generate the same behavioral columns as `rec-algorithm/algorithm/feature/event_feature.py`:

- totals: `event_count`, value sum/mean, active days, unique scenes and counterpart count;
- time: first/last event, recency, and 1/7/30-day counts;
- actions: click, expose, buy, collect and stay counts, plus click rate.

Each event updates both its user and item snapshot. Redis keys are `feature:user:{id}` and `feature:item:{id}`. Raw serving keys remain compatible with rec-server (`user:{id}`, `item:{id}`, `event:{userId}:scene:type`, and `new:{scene}`).

## Durable Training Data

Both jobs append raw Kafka payloads under `hdfs://namenode:8020/openrec/raw/{user,item,event}` and feature snapshots under `/openrec/features`. Offline training should read raw user/item dimensions and join the latest snapshot per `(entityType, entityId)` using `asOfTime`. Checkpoints are stored separately; never use checkpoint files as training input.

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

Configure Kafka, Redis, HDFS, checkpoint paths, parallelism, and event lateness in each module's `src/main/resources/dp.properties`. Use distinct checkpoint directories when comparing engines. Running both against the same topics is suitable for parity testing but duplicates persisted output.

## Testing

`feature-core` unit tests validate user/item aggregation, rolling windows, action counts, and click-rate semantics. When changing a feature in `rec-algorithm`, update the shared contract and its tests in the same change so online and offline definitions stay aligned.
