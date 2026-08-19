package com.openrec.dp.spark;

import static org.apache.spark.sql.functions.col;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

import com.openrec.dp.feature.EventFeatureAccumulator;
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
        List<StreamingQuery> queries = new ArrayList<>();
        Dataset<Row> users = kafka(spark, p, "kafka.user.topic");
        Dataset<Row> items = kafka(spark, p, "kafka.item.topic");
        Dataset<Row> events = kafka(spark, p, "kafka.event.topic");
        queries.add(RedisBatchWriter.persistRaw(users, "user", p));
        queries.add(RedisBatchWriter.persistRaw(items, "item", p));
        queries.add(RedisBatchWriter.persistRaw(events, "event", p));

        Dataset<FeatureUpdate> updates = events.flatMap((FlatMapFunction<Row, FeatureUpdate>) row -> {
            Event event = FeatureJson.fromJson(row.getString(0), Event.class);
            return event == null ? java.util.Collections.emptyIterator()
                : FeatureUpdates.fromEvent(event).iterator();
        }, Encoders.kryo(FeatureUpdate.class));
        Dataset<FeatureSnapshot> snapshots = updates
            .groupByKey((MapFunction<FeatureUpdate, String>) FeatureUpdate::key, Encoders.STRING())
            .flatMapGroupsWithState(SparkFeatureJob::aggregate, OutputMode.Update(),
                Encoders.kryo(EventFeatureAccumulator.class), Encoders.bean(FeatureSnapshot.class),
                GroupStateTimeout.NoTimeout());
        queries.add(RedisBatchWriter.persistSnapshots(snapshots, p));
        spark.streams().awaitAnyTermination();
    }

    private static Iterator<FeatureSnapshot> aggregate(String key, Iterator<FeatureUpdate> values,
        GroupState<EventFeatureAccumulator> state) {
        EventFeatureAccumulator accumulator = state.exists() ? state.get() : new EventFeatureAccumulator();
        FeatureSnapshot latest = null;
        while (values.hasNext()) { latest = accumulator.add(values.next()); }
        state.update(accumulator);
        return latest == null ? java.util.Collections.emptyIterator()
            : java.util.Collections.singletonList(latest).iterator();
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
