package com.openrec.dp.spark;

import static org.apache.spark.sql.functions.col;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import scala.Tuple2;

import com.openrec.dp.feature.EventFeatureAccumulator;
import com.openrec.dp.feature.FeatureCatalogContract;
import com.openrec.dp.feature.EntityMessage;
import com.openrec.dp.feature.FeatureJson;
import com.openrec.dp.feature.FeatureSnapshot;
import com.openrec.dp.feature.FeatureUpdate;
import com.openrec.dp.feature.FeatureUpdates;
import com.openrec.proto.model.Event;

/** Spark Structured Streaming implementation of the same feature-core contract as Flink. */
public class SparkFeatureJob {
    public static void main(String[] args) throws Exception {
        Properties p = properties();
        SparkSession spark = SparkSession.builder().appName("openrec-spark-realtime-features")
            .master(p.getProperty("spark.master")).getOrCreate();
        // Object-layout-dependent Kryo checkpoints cannot survive feature schema upgrades.
        // Keep them intact and seed the versioned JSON state from durable mutation history.
        String checkpoint = featureCheckpoint(p.getProperty("checkpoint.path"));
        p.setProperty("feature.checkpoint.path", checkpoint);
        Dataset<Tuple2<String, String>> initial = initialState(spark, p, checkpoint);
        KeyValueGroupedDataset<String, String> groupedInitial = initial.groupByKey(
            (MapFunction<Tuple2<String, String>, String>) Tuple2::_1, Encoders.STRING())
            .mapValues((MapFunction<Tuple2<String, String>, String>) Tuple2::_2, Encoders.STRING());
        List<StreamingQuery> queries = new ArrayList<>();
        Dataset<Row> users = kafka(spark, p, "kafka.user.topic");
        Dataset<Row> items = kafka(spark, p, "kafka.item.topic");
        Dataset<Row> events = kafka(spark, p, "kafka.event.topic");
        queries.add(RedisBatchWriter.persistRaw(users, "user", p));
        queries.add(RedisBatchWriter.persistRaw(items, "item", p));
        queries.add(RedisBatchWriter.persistRaw(events, "event", p));

        Dataset<FeatureUpdate> updates = updates(events);
        Dataset<FeatureSnapshot> snapshots = updates
            .groupByKey((MapFunction<FeatureUpdate, String>) FeatureUpdate::key, Encoders.STRING())
            .flatMapGroupsWithState(SparkFeatureJob::aggregate, OutputMode.Update(),
                Encoders.STRING(), Encoders.bean(FeatureSnapshot.class),
                GroupStateTimeout.NoTimeout(), groupedInitial);
        queries.add(RedisBatchWriter.persistSnapshots(snapshots, p));
        spark.streams().awaitAnyTermination();
    }

    static String featureCheckpoint(String root) {
        return root + "/features-json-v1-catalog-" + FeatureCatalogContract.get().getVersion();
    }

    static Dataset<FeatureUpdate> updates(Dataset<Row> events) {
        return events.flatMap((FlatMapFunction<Row, FeatureUpdate>) row -> {
            EntityMessage message = EntityMessage.parse("event", row.getString(0));
            Event event = message == null ? null
                : FeatureJson.fromJson(message.getDataJson(), Event.class);
            return event == null ? java.util.Collections.emptyIterator()
                : FeatureUpdates.fromEvent(event, message.isDelete(), message.getOccurredAt()).iterator();
        }, Encoders.kryo(FeatureUpdate.class));
    }

    static Dataset<Tuple2<String, String>> rebuildState(Dataset<FeatureUpdate> updates) {
        return updates.groupByKey((MapFunction<FeatureUpdate, String>) FeatureUpdate::key, Encoders.STRING())
            .mapGroups((MapGroupsFunction<String, FeatureUpdate, Tuple2<String, String>>) (key, values) -> {
                EventFeatureAccumulator accumulator = new EventFeatureAccumulator();
                while (values.hasNext()) { accumulator.add(values.next()); }
                return new Tuple2<>(key, FeatureJson.toJson(accumulator));
            }, Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
    }

    private static Dataset<Tuple2<String, String>> initialState(SparkSession spark, Properties p,
        String checkpoint) throws Exception {
        Path commits = new Path(checkpoint + "/commits");
        FileSystem fs = commits.getFileSystem(spark.sparkContext().hadoopConfiguration());
        if (fs.exists(commits) && fs.listStatus(commits, path -> path.getName().matches("[0-9]+")).length > 0) {
            return spark.emptyDataset(Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
        }
        Path history = new Path(p.getProperty("hdfs.output") + "/hive/event");
        if (!fs.exists(history)) {
            if (fs.exists(new Path(p.getProperty("checkpoint.path") + "/features"))) {
                throw new IllegalStateException("Legacy feature state needs durable event history for migration: "
                    + history);
            }
            return spark.emptyDataset(Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
        }
        Dataset<Tuple2<String, String>> initial = rebuildState(updates(
            spark.read().text(history.toString()).select(col("value").alias("json"))));
        // Refresh inactive entities as well; waiting for their next Kafka event is insufficient.
        RedisBatchWriter.persistSnapshotBatch(initial.map(
            (MapFunction<Tuple2<String, String>, FeatureSnapshot>) value ->
                FeatureJson.fromJson(value._2(), EventFeatureAccumulator.class).currentSnapshot(),
            Encoders.bean(FeatureSnapshot.class)), p);
        return initial;
    }

    static Iterator<FeatureSnapshot> aggregate(String key, Iterator<FeatureUpdate> values,
        GroupState<String> state) {
        EventFeatureAccumulator accumulator = state.exists()
            ? FeatureJson.fromJson(state.get(), EventFeatureAccumulator.class) : new EventFeatureAccumulator();
        if (accumulator == null) { throw new IllegalStateException("Invalid feature JSON state for " + key); }
        FeatureSnapshot latest = null;
        while (values.hasNext()) { accumulator.add(values.next()); latest = accumulator.currentSnapshot(); }
        state.update(FeatureJson.toJson(accumulator));
        return latest == null ? java.util.Collections.emptyIterator()
            : java.util.Collections.singletonList(latest).iterator();
    }

    /** Deterministic batch adapter used by the cross-engine feature parity gate. */
    public static FeatureSnapshot aggregateForParity(Iterable<FeatureUpdate> values, long asOfTime) {
        EventFeatureAccumulator accumulator = new EventFeatureAccumulator();
        for (FeatureUpdate value : values) { accumulator.add(value); }
        return accumulator.snapshot(asOfTime);
    }

    /** Exercise Spark's real typed shuffle/group execution for parity and micro-batch tests. */
    public static Dataset<FeatureSnapshot> aggregateBatchForParity(
        Dataset<FeatureUpdate> updates, final long asOfTime) {
        KeyValueGroupedDataset<String, FeatureUpdate> grouped = updates.groupByKey(
            (MapFunction<FeatureUpdate, String>) FeatureUpdate::key, Encoders.STRING());
        return grouped.mapGroups((MapGroupsFunction<String, FeatureUpdate, FeatureSnapshot>) (key, values) -> {
            EventFeatureAccumulator accumulator = new EventFeatureAccumulator();
            while (values.hasNext()) { accumulator.add(values.next()); }
            return accumulator.snapshot(asOfTime);
        }, Encoders.bean(FeatureSnapshot.class));
    }

    private static Dataset<Row> kafka(SparkSession spark, Properties p, String topic) {
        return spark.readStream().format("kafka")
            .option("kafka.bootstrap.servers", p.getProperty("kafka.servers"))
            .option("subscribe", p.getProperty(topic)).option("startingOffsets", "earliest")
            .load().selectExpr("CAST(value AS STRING) AS json");
    }

    private static Properties properties() throws Exception {
        Properties p = new Properties();
        try (InputStream in = SparkFeatureJob.class.getClassLoader().getResourceAsStream("dp.properties")) {
            if (in == null) { throw new IllegalStateException("dp.properties not found"); }
            p.load(in);
        }
        return p;
    }
}
