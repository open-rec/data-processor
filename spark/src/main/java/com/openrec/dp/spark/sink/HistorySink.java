package com.openrec.dp.spark.sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

import com.openrec.dp.feature.EntityPartitions;
import com.openrec.dp.feature.FeatureSnapshot;

public final class HistorySink {
    private HistorySink() {}

    public static void writeEntities(Dataset<Row> batch, String type, String output) {
        batch
            .select(col("json"),
                udf((String json) -> EntityPartitions.bucket(type, json), DataTypes.StringType).apply(col("json"))
                    .alias("bucket"))
            .withColumn("dt", org.apache.spark.sql.functions.regexp_replace(col("bucket"), "^dt=", "")).drop("bucket")
            .write().mode("append").partitionBy("dt").text(output);
    }

    public static void writeSnapshots(Dataset<FeatureSnapshot> batch, String output) {
        batch.toJSON().write().mode("append").text(output);
    }
}
