package com.openrec.dp.spark;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.KeyValueGroupedDataset;

import com.openrec.dp.spark.source.KafkaEntitySource;
import com.openrec.dp.spark.job.EntityProjectionJob;
import com.openrec.dp.spark.job.FeatureAggregationJob;

/** Compatible entry point assembling entity projection and feature aggregation. */
public class SparkFeatureJob {
    public static void main(String[] args) throws Exception {
        Properties p = properties();
        if (args.length > 0) {
            try (InputStream in = java.nio.file.Files.newInputStream(java.nio.file.Paths.get(args[0]))) {
                p.load(in);
            }
        }
        SparkSession spark = SparkSession.builder().appName("openrec-spark-realtime-features")
            .master(p.getProperty("spark.master")).getOrCreate();
        KeyValueGroupedDataset<String, String> initial = FeatureAggregationJob.restore(spark, p);
        List<StreamingQuery> queries = new ArrayList<>();
        Dataset<Row> users = KafkaEntitySource.read(spark, p, "kafka.user.topic");
        Dataset<Row> items = KafkaEntitySource.read(spark, p, "kafka.item.topic");
        Dataset<Row> events = KafkaEntitySource.read(spark, p, "kafka.event.topic");
        queries.add(EntityProjectionJob.start(users, "user", p));
        queries.add(EntityProjectionJob.start(items, "item", p));
        queries.add(EntityProjectionJob.start(events, "event", p));

        queries.add(FeatureAggregationJob.start(events, p, initial));
        spark.streams().awaitAnyTermination();
    }

    private static Properties properties() throws Exception {
        Properties p = new Properties();
        try (InputStream in = SparkFeatureJob.class.getClassLoader().getResourceAsStream("dp.properties")) {
            if (in == null) {
                throw new IllegalStateException("dp.properties not found");
            }
            p.load(in);
        }
        return p;
    }
}
