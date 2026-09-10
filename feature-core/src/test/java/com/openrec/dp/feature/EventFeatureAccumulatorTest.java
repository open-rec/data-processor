package com.openrec.dp.feature;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.io.InputStreamReader;

import org.junit.Test;

import com.openrec.proto.model.Event;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class EventFeatureAccumulatorTest {
    @Test
    public void matchesOfflineEventFeatureColumnsForUserAndItem() {
        Event click = event("u", "i1", "s1", "click", "2", "200000");
        Event expose = event("u", "i2", "s2", "expose", "0", "150000");
        List<FeatureUpdate> clickUpdates = FeatureUpdates.fromEvent(click);
        List<FeatureUpdate> exposeUpdates = FeatureUpdates.fromEvent(expose);

        EventFeatureAccumulator user = new EventFeatureAccumulator();
        user.add(exposeUpdates.get(0));
        FeatureSnapshot snapshot = user.add(clickUpdates.get(0));
        assertEquals(2d, snapshot.getFeatures().get("event_count"), 0d);
        assertEquals(2d, snapshot.getFeatures().get("event_value_sum"), 0d);
        assertEquals(2d, snapshot.getFeatures().get("event_active_days"), 0d);
        assertEquals(2d, snapshot.getFeatures().get("event_unique_scene_count"), 0d);
        assertEquals(2d, snapshot.getFeatures().get("event_unique_item_count"), 0d);
        assertEquals(0.5d, snapshot.getFeatures().get("event_click_rate"), 0d);
        assertEquals(FeatureCatalogContract.get().getVersion(), snapshot.getCatalogVersion());
        assertEquals(FeatureCatalogContract.get().getSha256(), snapshot.getCatalogSha256());
        assertEquals(new LinkedHashSet<>(Arrays.asList(
            "event_count", "event_value_sum", "event_value_mean", "event_active_days",
            "event_unique_scene_count", "event_unique_item_count", "event_first_time",
            "event_last_time", "event_recency_seconds", "event_count_1d", "event_count_7d",
            "event_count_30d", "event_click_count", "event_expose_count", "event_buy_count",
            "event_collect_count", "event_stay_count", "event_click_rate")),
            snapshot.getFeatures().keySet());

        EventFeatureAccumulator item = new EventFeatureAccumulator();
        FeatureSnapshot itemSnapshot = item.add(clickUpdates.get(1));
        assertEquals(1d, itemSnapshot.getFeatures().get("event_unique_user_count"), 0d);
    }

    @Test
    public void rejectsMalformedAndIncompleteEvents() {
        assertNull(FeatureJson.fromJson("{bad-json", Event.class));
        assertEquals(0, FeatureUpdates.fromEvent(new Event()).size());
        Event invalidTime = event("u", "i", "s", "click", "1", "bad");
        assertEquals(0, FeatureUpdates.fromEvent(invalidTime).size());
    }

    @Test
    public void deduplicatesTraceAndIgnoresEmptyScene() {
        Event event = event("u", "i", " ", "click", "2", "100");
        event.setTraceId("same");
        FeatureUpdate update = FeatureUpdates.fromEvent(event).get(0);
        EventFeatureAccumulator accumulator = new EventFeatureAccumulator();
        accumulator.add(update);
        FeatureSnapshot result = accumulator.add(update);
        assertEquals(1d, result.getFeatures().get("event_count"), 0d);
        assertEquals(0d, result.getFeatures().get("event_unique_scene_count"), 0d);
    }

    @Test
    public void matchesSharedPythonGoldenFixture() {
        JsonObject fixture = new JsonParser().parse(new InputStreamReader(
            getClass().getClassLoader().getResourceAsStream("event-feature-parity.json")))
            .getAsJsonObject();
        long asOf = fixture.get("as_of_time").getAsLong();
        EventFeatureAccumulator accumulator = new EventFeatureAccumulator();
        for (JsonElement element : fixture.getAsJsonArray("events")) {
            JsonObject value = element.getAsJsonObject();
            Event event = event(value.get("user_id").getAsString(), value.get("item_id").getAsString(),
                value.get("scene").getAsString(), value.get("type").getAsString(),
                value.get("value").getAsString(), value.get("time").getAsString());
            event.setTraceId(value.get("trace_id").getAsString());
            for (FeatureUpdate update : FeatureUpdates.fromEvent(event)) {
                if ("user".equals(update.getEntityType()) && update.getEventTime() <= asOf) {
                    accumulator.add(update);
                }
            }
        }
        FeatureSnapshot snapshot = accumulator.snapshot(asOf);
        JsonObject expected = fixture.getAsJsonObject("expected_user");
        for (String name : snapshot.getFeatures().keySet()) {
            assertEquals(name, expected.get(name).getAsDouble(),
                snapshot.getFeatures().get(name), 0d);
        }
    }

    private static Event event(String user, String item, String scene, String type,
        String value, String time) {
        Event event = new Event();
        event.setUserId(user); event.setItemId(item); event.setScene(scene);
        event.setType(type); event.setValue(value); event.setTime(time);
        return event;
    }
}
