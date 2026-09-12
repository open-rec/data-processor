package com.openrec.dp.flink.process;

import static org.junit.Assert.assertEquals;

import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Test;

import com.openrec.dp.feature.FeatureSnapshot;
import com.openrec.dp.feature.FeatureUpdate;
import com.openrec.dp.feature.FeatureUpdates;
import com.openrec.proto.model.Event;

public class FlinkFeatureOperatorStateTest {
    @Test public void keyedStateHandlesOutOfOrderMutationAndDeleteTie() throws Exception {
        EventFeatureProcessFunction function = new EventFeatureProcessFunction();
        KeyedProcessOperator<String, FeatureUpdate, FeatureSnapshot> operator =
            new KeyedProcessOperator<>(function);
        KeyedOneInputStreamOperatorTestHarness<String, FeatureUpdate, FeatureSnapshot> harness =
            new KeyedOneInputStreamOperatorTestHarness<>(operator, FeatureUpdate::key,
                org.apache.flink.api.common.typeinfo.Types.STRING);
        harness.open();
        Event event = event();
        harness.processElement(FeatureUpdates.fromEvent(event, false, 100).get(0), 100);
        harness.processElement(FeatureUpdates.fromEvent(event, false, 90).get(0), 90);
        harness.processElement(FeatureUpdates.fromEvent(event, false, 200).get(0), 200);
        harness.processElement(FeatureUpdates.fromEvent(event, true, 200).get(0), 200);
        FeatureSnapshot latest = null;
        for (Object value : harness.getOutput()) {
            if (value instanceof org.apache.flink.streaming.runtime.streamrecord.StreamRecord) {
                latest = (FeatureSnapshot) ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) value)
                    .getValue();
            }
        }
        assertEquals(0d, latest.getFeatures().get("event_count"), 0d);
        harness.close();
    }

    private Event event() {
        Event event = new Event();
        event.setEventId("event-1"); event.setUserId("u"); event.setItemId("i");
        event.setScene("s"); event.setType("click"); event.setValue("1"); event.setTime("50");
        return event;
    }
}
