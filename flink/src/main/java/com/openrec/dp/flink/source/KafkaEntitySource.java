package com.openrec.dp.flink.source;

import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public final class KafkaEntitySource {
    private KafkaEntitySource() {}

    public static DataStream<String> read(StreamExecutionEnvironment env, Properties p, String topicProperty,
        String name) {
        String groupId = p.getProperty("kafka.groupId") + "-flink";
        KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers(p.getProperty("kafka.servers"))
            .setTopics(p.getProperty(topicProperty)).setGroupId(groupId).setClientIdPrefix(groupId + "-" + name)
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new SimpleStringSchema()).build();
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-" + name);
    }
}
