package com.openrec.dp.spark.job;

import com.openrec.dp.spark.process.FeatureAggregationProcess;
import com.openrec.dp.spark.source.HistoryEntitySource;

import java.util.Properties;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import scala.Tuple2;

import com.openrec.dp.feature.EventFeatureAccumulator;
import com.openrec.dp.feature.FeatureCatalogContract;
import com.openrec.dp.feature.FeatureJson;
import com.openrec.dp.feature.FeatureSnapshot;
import com.openrec.dp.spark.sink.SnapshotSink;

public final class FeatureRebuildJob {
    private FeatureRebuildJob() {}

    public static String featureCheckpoint(String root) {
        return root + "/features-json-v1-catalog-" + FeatureCatalogContract.get().getVersion();
    }

    public static Dataset<Tuple2<String, String>> initialState(SparkSession spark, Properties p, String checkpoint)
        throws Exception {
        Path commits = new Path(checkpoint + "/commits");
        FileSystem fs = commits.getFileSystem(spark.sparkContext().hadoopConfiguration());
        if (fs.exists(commits) && fs.listStatus(commits, path -> path.getName().matches("[0-9]+")).length > 0) {
            return spark.emptyDataset(Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
        }
        Path history = new Path(p.getProperty("hdfs.output") + "/hive/event");
        if (!fs.exists(history)) {
            if (fs.exists(new Path(p.getProperty("checkpoint.path") + "/features"))) {
                throw new IllegalStateException(
                    "Legacy feature state needs durable event history for migration: " + history);
            }
            return spark.emptyDataset(Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
        }
        Dataset<Tuple2<String, String>> initial = FeatureAggregationProcess
            .rebuildState(FeatureAggregationProcess.updates(HistoryEntitySource.read(spark, history.toString())));
        // Refresh inactive entities as well; waiting for their next Kafka event is insufficient.
        SnapshotSink.writeBatch(initial.map(
            (MapFunction<Tuple2<String, String>, FeatureSnapshot>)value -> FeatureJson
                .fromJson(value._2(), EventFeatureAccumulator.class).currentSnapshot(),
            Encoders.bean(FeatureSnapshot.class)), p);
        return initial;
    }
}
