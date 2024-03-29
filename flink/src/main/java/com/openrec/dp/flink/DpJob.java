package com.openrec.dp.flink;

import com.openrec.dp.flink.process.map.StringToEventFunction;
import com.openrec.dp.flink.process.map.StringToItemFunction;
import com.openrec.dp.flink.process.map.StringToUserFunction;
import com.openrec.dp.flink.util.FileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.hbase.sink.HBaseSinkFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

@Slf4j
public class DpJob {

    private static final String DP_PROPERTIES = "dp.properties";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Runtime.getRuntime().availableProcessors());

        Properties properties = FileUtil.loadProperties(DP_PROPERTIES);
        if (properties == null) {
            log.error("load {} failed, cannot start job", DP_PROPERTIES);
            System.exit(-1);
        }

        KafkaSource<String> itemSource = KafkaSource.<String>builder()
                .setBootstrapServers(properties.getProperty("kafka.servers"))
                .setTopics(properties.getProperty("kafka.item.topic"))
                .setGroupId(properties.getProperty("kafka.groupId"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> itemStream = env.fromSource(itemSource, WatermarkStrategy.noWatermarks(), "kafka-item");
        itemStream.map(new StringToItemFunction())
                .addSink(new HBaseSinkFunction<>(null, null, null, 0, 0, 0));
//        itemStream.addSink(new NewRedisSink(new FlinkJedisPoolConfig.Builder()
//                .build()));

        KafkaSource<String> userSource = KafkaSource.<String>builder()
                .setBootstrapServers(properties.getProperty("kafka.servers"))
                .setTopics(properties.getProperty("kafka.user.topic"))
                .setGroupId(properties.getProperty("kafka.groupId"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> userStream = env.fromSource(userSource, WatermarkStrategy.noWatermarks(), "kafka-user");
        userStream.map(new StringToUserFunction())
                .addSink(new HBaseSinkFunction<>(null, null, null, 0, 0, 0));


        KafkaSource<String> eventSource = KafkaSource.<String>builder()
                .setBootstrapServers(properties.getProperty("kafka.servers"))
                .setTopics(properties.getProperty("kafka.event.topic"))
                .setGroupId(properties.getProperty("kafka.groupId"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> eventStream = env.fromSource(eventSource, WatermarkStrategy.noWatermarks(), "kafka-event");
        eventStream.map(new StringToEventFunction())
                .addSink(new HBaseSinkFunction<>(null, null, null, 0, 0, 0));

        try {
            env.execute();
            env.enableCheckpointing(6000, CheckpointingMode.EXACTLY_ONCE);
        } catch (Exception e) {
            log.error("flink job execute failed:{}", ExceptionUtils.getStackTrace(e));
        }
    }
}
