package com.openrec.dp.spark;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import com.openrec.dp.feature.FeatureSnapshot;
import com.openrec.dp.feature.FeatureJson;
import com.openrec.dp.feature.EventFeatureAccumulator;
import com.openrec.dp.feature.FeatureUpdate;
import com.openrec.dp.feature.FeatureUpdates;
import com.openrec.proto.model.Event;

public class SparkFeatureMicroBatchTest {
    @Test public void typedMicroBatchRunsThroughSparkGrouping() {
        SparkSession spark = SparkSession.builder().master("local[2]")
            .appName("openrec-feature-parity-test").config("spark.ui.enabled", "false").getOrCreate();
        try {
            Event event = new Event();
            event.setEventId("event-1"); event.setUserId("u"); event.setItemId("i");
            event.setScene("s"); event.setType("click"); event.setValue("1"); event.setTime("50");
            FeatureUpdate insert = FeatureUpdates.fromEvent(event, false, 100).get(0);
            FeatureUpdate stale = FeatureUpdates.fromEvent(event, false, 90).get(0);
            FeatureUpdate delete = FeatureUpdates.fromEvent(event, true, 200).get(0);
            Dataset<FeatureUpdate> input = spark.createDataset(
                Arrays.asList(delete, stale, insert), Encoders.kryo(FeatureUpdate.class));
            FeatureSnapshot result = SparkFeatureJob.aggregateBatchForParity(input, 200).first();
            assertEquals(0d, result.getFeatures().get("event_count"), 0d);

            // Migration replays out-of-order history, including tombstones; JSON state
            // must preserve the dedup metadata when Kafka replays older mutations.
            String saved = SparkFeatureJob.rebuildState(input).first()._2();
            EventFeatureAccumulator restored = FeatureJson.fromJson(saved, EventFeatureAccumulator.class);
            restored.add(insert);
            assertEquals(0d, restored.snapshot(200).getFeatures().get("event_count"), 0d);
            event.setEventId("event-2");
            FeatureUpdate next = FeatureUpdates.fromEvent(event, false, 300).get(0);
            restored.add(next);
            EventFeatureAccumulator restarted = FeatureJson.fromJson(
                FeatureJson.toJson(restored), EventFeatureAccumulator.class);
            restarted.add(next);
            FeatureSnapshot replayed = restarted.snapshot(200);
            assertEquals(1d, replayed.getFeatures().get("event_count"), 0d);
            assertEquals(1d, replayed.getRecentEventStats().get(50L).get("count"), 0d);
        } finally {
            spark.stop();
        }
    }
}
