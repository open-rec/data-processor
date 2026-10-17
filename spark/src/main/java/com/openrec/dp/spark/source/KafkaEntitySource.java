package com.openrec.dp.spark.source;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class KafkaEntitySource {
    private KafkaEntitySource() {}

    public static Dataset<Row> read(SparkSession spark, Properties p, String topic) {
        return spark.readStream().format("kafka").option("kafka.bootstrap.servers", p.getProperty("kafka.servers"))
            .option("subscribe", p.getProperty(topic)).option("startingOffsets", "earliest").load()
            .selectExpr("CAST(value AS STRING) AS json");
    }
}
