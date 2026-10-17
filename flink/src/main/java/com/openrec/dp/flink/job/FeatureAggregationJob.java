package com.openrec.dp.flink.job;

import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;

import com.openrec.dp.feature.FeatureJson;
import com.openrec.dp.feature.FeatureSnapshot;
import com.openrec.dp.flink.sink.SnapshotRedisSink;
import com.openrec.dp.flink.process.FeatureAggregationProcess;
import com.openrec.dp.flink.sink.HistoryFileSink;

public final class FeatureAggregationJob {
    private FeatureAggregationJob() {}

    public static void attach(DataStream<String> events, Properties p) {
        DataStream<FeatureSnapshot> snapshots = FeatureAggregationProcess.apply(events, p);
        snapshots.addSink(new SnapshotRedisSink(p)).name("feature-snapshot-redis");
        snapshots.map(FeatureJson::toJson).sinkTo(HistoryFileSink.fileSink(p, "features"));
    }
}
