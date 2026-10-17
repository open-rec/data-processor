package com.openrec.dp.spark.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

/** Bounded mutation history with the same json column as the Kafka source. */
public final class HistoryEntitySource {
    private HistoryEntitySource() {}

    public static Dataset<Row> read(SparkSession spark, String path) {
        return spark.read().text(path).select(col("value").alias("json"));
    }
}
