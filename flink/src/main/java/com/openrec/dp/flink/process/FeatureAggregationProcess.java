package com.openrec.dp.flink.process;

import java.time.Duration;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;

import com.openrec.dp.feature.FeatureSnapshot;
import com.openrec.dp.feature.FeatureUpdate;

public final class FeatureAggregationProcess {
    private FeatureAggregationProcess() {}

    public static DataStream<FeatureSnapshot> apply(DataStream<String> events, Properties p) {
        return events.flatMap(new EventToFeatureUpdates()).name("event-to-user-and-item-updates")
            .assignTimestampsAndWatermarks(WatermarkStrategy
                .<FeatureUpdate>forBoundedOutOfOrderness(
                    Duration.ofSeconds(Long.parseLong(p.getProperty("event.out-of-order.seconds", "30"))))
                .withTimestampAssigner((update, timestamp) -> update.getEventTime() * 1000L))
            .keyBy(FeatureUpdate::key).process(new EventFeatureProcessFunction());
    }
}
