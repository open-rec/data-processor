package com.openrec.dp.flink;

import java.util.Properties;

import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.openrec.dp.flink.util.FileUtil;
import com.openrec.dp.flink.source.KafkaEntitySource;
import com.openrec.dp.flink.job.EntityProjectionJob;
import com.openrec.dp.flink.job.FeatureAggregationJob;

/** Compatible entry point assembling entity projection and feature aggregation. */
public class DpJob {
    public static void main(String[] args) throws Exception {
        Properties p = FileUtil.loadProperties("dp.properties");
        if (p == null) {
            throw new IllegalStateException("dp.properties not found");
        }
        if (args.length > 0) {
            try (java.io.InputStream in = java.nio.file.Files.newInputStream(java.nio.file.Paths.get(args[0]))) {
                p.load(in);
            }
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Integer.parseInt(p.getProperty("job.parallelism", "2")));
        env.enableCheckpointing(Long.parseLong(p.getProperty("checkpoint.interval.ms", "60000")),
            CheckpointingMode.EXACTLY_ONCE);

        DataStream<String> users = KafkaEntitySource.read(env, p, "kafka.user.topic", "user");
        DataStream<String> items = KafkaEntitySource.read(env, p, "kafka.item.topic", "item");
        DataStream<String> events = KafkaEntitySource.read(env, p, "kafka.event.topic", "event");
        EntityProjectionJob.attach(users, "user", p);
        EntityProjectionJob.attach(items, "item", p);
        EntityProjectionJob.attach(events, "event", p);

        FeatureAggregationJob.attach(events, p);
        env.execute("openrec-flink-realtime-features");
    }
}
