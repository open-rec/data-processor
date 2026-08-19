package com.openrec.dp.flink;

import java.time.Duration;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import com.openrec.dp.feature.FeatureJson;
import com.openrec.dp.feature.FeatureSnapshot;
import com.openrec.dp.feature.FeatureUpdate;
import com.openrec.dp.flink.process.EventFeatureProcessFunction;
import com.openrec.dp.flink.process.EventToFeatureUpdates;
import com.openrec.dp.flink.sink.RawRedisSink;
import com.openrec.dp.flink.sink.HBaseEntitySink;
import com.openrec.dp.flink.sink.SnapshotRedisSink;
import com.openrec.dp.flink.util.FileUtil;

/** Kafka real-time feature pipeline. Raw records and snapshots are both persisted to HDFS. */
public class DpJob {
    public static void main(String[] args) throws Exception {
        Properties p = FileUtil.loadProperties("dp.properties");
        if (p == null) { throw new IllegalStateException("dp.properties not found"); }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Integer.parseInt(p.getProperty("job.parallelism", "2")));
        env.enableCheckpointing(Long.parseLong(p.getProperty("checkpoint.interval.ms", "60000")),
            CheckpointingMode.EXACTLY_ONCE);

        DataStream<String> users = source(env, p, "kafka.user.topic", "user");
        DataStream<String> items = source(env, p, "kafka.item.topic", "item");
        DataStream<String> events = source(env, p, "kafka.event.topic", "event");
        persistRaw(users, "user", p);
        persistRaw(items, "item", p);
        persistRaw(events, "event", p);

        DataStream<FeatureSnapshot> snapshots = events.flatMap(new EventToFeatureUpdates())
            .name("event-to-user-and-item-updates")
            .assignTimestampsAndWatermarks(WatermarkStrategy.<FeatureUpdate>forBoundedOutOfOrderness(
                Duration.ofSeconds(Long.parseLong(p.getProperty("event.out-of-order.seconds", "30"))))
                .withTimestampAssigner((update, timestamp) -> update.getEventTime() * 1000L))
            .keyBy(FeatureUpdate::key)
            .process(new EventFeatureProcessFunction());
        snapshots.addSink(new SnapshotRedisSink(p)).name("feature-snapshot-redis");
        snapshots.map(FeatureJson::toJson).sinkTo(fileSink(p, "features"));
        env.execute("openrec-flink-realtime-features");
    }

    private static DataStream<String> source(StreamExecutionEnvironment env, Properties p,
        String topicProperty, String name) {
        String groupId = p.getProperty("kafka.groupId") + "-flink";
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(p.getProperty("kafka.servers"))
            .setTopics(p.getProperty(topicProperty))
            .setGroupId(groupId)
            .setClientIdPrefix(groupId + "-" + name)
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new SimpleStringSchema()).build();
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-" + name);
    }

    private static void persistRaw(DataStream<String> stream, String type, Properties p) {
        stream.addSink(new RawRedisSink(type, p)).name(type + "-serving-redis");
        if (Boolean.parseBoolean(p.getProperty("hbase.enabled", "true"))) {
            stream.addSink(new HBaseEntitySink(type, p)).name(type + "-entity-hbase");
        }
        if (Boolean.parseBoolean(p.getProperty("hive.enabled", "true"))) {
            stream.sinkTo(fileSink(p, "hive/" + type));
        }
    }

    private static FileSink<String> fileSink(Properties p, String path) {
        return FileSink.forRowFormat(new Path(p.getProperty("hdfs.output") + "/" + path),
            new SimpleStringEncoder<String>("UTF-8")).build();
    }
}
