package com.openrec.dp.spark.sink;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.streaming.StreamingQuery;

import com.openrec.dp.feature.FeatureSnapshot;

public final class SnapshotSink {
    private SnapshotSink() {}

    public static StreamingQuery attach(Dataset<FeatureSnapshot> input, Properties p) throws Exception {
        String checkpoint = p.getProperty("feature.checkpoint.path");
        return input.writeStream().outputMode("update").option("checkpointLocation", checkpoint)
            .foreachBatch((batch, id) -> {
                writeBatch(batch, p);
            }).start();
    }

    public static void writeBatch(Dataset<FeatureSnapshot> batch, Properties p) {
        HistorySink.writeSnapshots(batch, p.getProperty("hdfs.output") + "/features");
        RedisSnapshotSink.write(batch, p);
    }
}
