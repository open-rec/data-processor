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
        List<FeatureUpdate> userUpdates = new ArrayList<>();
        for (FeatureUpdate update : updates) {
            if ("user".equals(update.getEntityType())) { userUpdates.add(update); }
        }
        FeatureSnapshot actual = SparkFeatureJob.aggregateForParity(userUpdates, asOf);
        JsonObject expected = fixture.getAsJsonObject("expected_user");
        assertEquals(expected.size(), actual.getFeatures().size());
        for (Map.Entry<String, JsonElement> entry : expected.entrySet()) {
            String name = entry.getKey();
            assertEquals(name, entry.getValue().getAsDouble(), actual.getFeatures().get(name), 0d);
        }
        assertItems(fixture, updates, asOf);
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
            if (value.has("event_id")) { event.setEventId(value.get("event_id").getAsString()); }
            event.setUserId(value.get("user_id").getAsString());
            event.setItemId(value.get("item_id").getAsString());
            event.setScene(value.get("scene").getAsString());
            event.setType(value.get("type").getAsString());
            event.setValue(value.get("value").getAsString());
            event.setTime(value.get("time").getAsString());
            event.setTraceId(value.get("trace_id").getAsString());
            boolean deleted = value.has("operation")
                && "DELETE".equals(value.get("operation").getAsString());
            long mutationTime = value.has("occurred_at") ? value.get("occurred_at").getAsLong() : 0;
            for (FeatureUpdate update : FeatureUpdates.fromEvent(event, deleted, mutationTime)) {
                if (update.getEventTime() <= asOf) {
                    result.add(update);
                }
            }
        }
        return result;
    }

    private void assertItems(JsonObject fixture, List<FeatureUpdate> updates, long asOf) {
        for (Map.Entry<String, JsonElement> item : fixture.getAsJsonObject("expected_items").entrySet()) {
            List<FeatureUpdate> selected = new ArrayList<>();
            for (FeatureUpdate update : updates) {
                if ("item".equals(update.getEntityType()) && item.getKey().equals(update.getEntityId())) {
                    selected.add(update);
                }
            }
            FeatureSnapshot actual = SparkFeatureJob.aggregateForParity(selected, asOf);
            JsonObject expected = item.getValue().getAsJsonObject();
            assertEquals(expected.size(), actual.getFeatures().size());
            for (Map.Entry<String, JsonElement> entry : expected.entrySet()) {
                assertEquals(entry.getKey(), entry.getValue().getAsDouble(),
                    actual.getFeatures().get(entry.getKey()), 0d);
            }
        }
    }
}
