package com.openrec.dp.spark.job;

import java.util.Properties;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.KeyValueGroupedDataset;
import scala.Tuple2;

import com.openrec.dp.feature.FeatureSnapshot;
import com.openrec.dp.feature.FeatureUpdate;
import com.openrec.dp.spark.process.FeatureAggregationProcess;
import com.openrec.dp.spark.sink.SnapshotSink;

public final class FeatureAggregationJob {
    private FeatureAggregationJob() {}

    public static KeyValueGroupedDataset<String, String> restore(SparkSession spark, Properties p) throws Exception {
        String checkpoint = FeatureRebuildJob.featureCheckpoint(p.getProperty("checkpoint.path"));
        p.setProperty("feature.checkpoint.path", checkpoint);
        Dataset<Tuple2<String, String>> initial = FeatureRebuildJob.initialState(spark, p, checkpoint);
        KeyValueGroupedDataset<String, String> groupedInitial =
            initial.groupByKey((MapFunction<Tuple2<String, String>, String>)Tuple2::_1, Encoders.STRING())
                .mapValues((MapFunction<Tuple2<String, String>, String>)Tuple2::_2, Encoders.STRING());
        return groupedInitial;
    }

    public static StreamingQuery start(Dataset<Row> events, Properties p,
        KeyValueGroupedDataset<String, String> groupedInitial) throws Exception {
        Dataset<FeatureUpdate> updates = FeatureAggregationProcess.updates(events);
        Dataset<FeatureSnapshot> snapshots =
            updates.groupByKey((MapFunction<FeatureUpdate, String>)FeatureUpdate::key, Encoders.STRING())
                .flatMapGroupsWithState(FeatureAggregationProcess::aggregate, OutputMode.Update(), Encoders.STRING(),
                    Encoders.bean(FeatureSnapshot.class), GroupStateTimeout.NoTimeout(), groupedInitial);
        return SnapshotSink.attach(snapshots, p);
    }
}
