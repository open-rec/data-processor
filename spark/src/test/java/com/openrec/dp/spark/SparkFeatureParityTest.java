package com.openrec.dp.spark;

import static org.junit.Assert.assertEquals;

import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.openrec.dp.feature.FeatureSnapshot;
import com.openrec.dp.feature.FeatureUpdate;
import com.openrec.dp.feature.FeatureUpdates;
import com.openrec.proto.model.Event;

public class SparkFeatureParityTest {
    @Test public void matchesCanonicalPythonFixture() {
        JsonObject fixture = fixture();
        long asOf = fixture.get("as_of_time").getAsLong();
        List<FeatureUpdate> updates = updates(fixture, asOf);
        FeatureSnapshot actual = SparkFeatureJob.aggregateForParity(updates, asOf);
        JsonObject expected = fixture.getAsJsonObject("expected_user");
        assertEquals(expected.size(), actual.getFeatures().size());
        for (Map.Entry<String, JsonElement> entry : expected.entrySet()) {
            String name = entry.getKey();
            assertEquals(name, entry.getValue().getAsDouble(), actual.getFeatures().get(name), 0d);
        }
    }

    private JsonObject fixture() {
        return new JsonParser().parse(new InputStreamReader(getClass().getClassLoader()
            .getResourceAsStream("event-feature-parity.json"))).getAsJsonObject();
    }

    private List<FeatureUpdate> updates(JsonObject fixture, long asOf) {
        List<FeatureUpdate> result = new ArrayList<>();
        for (JsonElement element : fixture.getAsJsonArray("events")) {
            JsonObject value = element.getAsJsonObject();
            Event event = new Event();
            event.setUserId(value.get("user_id").getAsString());
            event.setItemId(value.get("item_id").getAsString());
            event.setScene(value.get("scene").getAsString());
            event.setType(value.get("type").getAsString());
            event.setValue(value.get("value").getAsString());
            event.setTime(value.get("time").getAsString());
            event.setTraceId(value.get("trace_id").getAsString());
            for (FeatureUpdate update : FeatureUpdates.fromEvent(event)) {
                if ("user".equals(update.getEntityType()) && update.getEventTime() <= asOf) {
                    result.add(update);
                }
            }
        }
        return result;
    }
}
