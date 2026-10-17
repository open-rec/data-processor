package com.openrec.dp.spark.job;

import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import com.openrec.dp.spark.sink.EntitySink;

/** Entity projection and history share the original raw-entity checkpoint boundary. */
public final class EntityProjectionJob {
    private EntityProjectionJob() {}

    public static StreamingQuery start(Dataset<Row> input, String type, Properties p) throws Exception {
        String checkpoint = p.getProperty("checkpoint.path") + "/raw-" + type;
        return input.writeStream().option("checkpointLocation", checkpoint).foreachBatch((batch, id) -> {
            EntitySink.writeBatch(batch, type, p);
        }).start();
    }
}
