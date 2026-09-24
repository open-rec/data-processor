package com.openrec.dp.feature;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;
import java.io.InputStreamReader;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

import com.openrec.proto.model.Event;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class EventFeatureAccumulatorTest {
    @Test
    public void emitsAnIndependentSessionSnapshotWhenSessionIdIsPresent() {
        Event event = event("u", "i", "s", "click", "2", "100");
        event.setSessionId("session-1");
        List<FeatureUpdate> updates = FeatureUpdates.fromEvent(event);
        assertEquals(3, updates.size());
        FeatureSnapshot snapshot = new EventFeatureAccumulator().add(updates.get(2));
        assertEquals("session", snapshot.getEntityType());
        assertEquals("session-1", snapshot.getEntityId());
        assertEquals(1d, snapshot.getFeatures().get("event_unique_item_count"), 0d);
        assertEquals(FeatureCatalogContract.get().getColumns("session"),
            snapshot.getFeatures().keySet());
    }

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
        assertEquals(1d, snapshot.getFeatures().get("event_ctr"), 0d);
        assertEquals(FeatureCatalogContract.get().getVersion(), snapshot.getCatalogVersion());
        assertEquals(FeatureCatalogContract.get().getSha256(), snapshot.getCatalogSha256());
        assertEquals(FeatureCatalogContract.get().getColumns("user"),
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
        event.setEventId("same");
        event.setTraceId("request");
        FeatureUpdate update = FeatureUpdates.fromEvent(event).get(0);
        EventFeatureAccumulator accumulator = new EventFeatureAccumulator();
        accumulator.add(update);
        FeatureSnapshot result = accumulator.add(update);
        assertEquals(1d, result.getFeatures().get("event_count"), 0d);
        assertEquals(0d, result.getFeatures().get("event_unique_scene_count"), 0d);
    }

    @Test
    public void traceContextDoesNotCollapseExposeAndClick() {
        Event expose = event("u", "i", "s", "expose", "0", "100");
        expose.setTraceId("request-1");
        Event click = event("u", "i", "s", "click", "1", "101");
        click.setTraceId("request-1");
        EventFeatureAccumulator accumulator = new EventFeatureAccumulator();
        accumulator.add(FeatureUpdates.fromEvent(expose).get(0));
        FeatureSnapshot result = accumulator.add(FeatureUpdates.fromEvent(click).get(0));
        assertEquals(2d, result.getFeatures().get("event_count"), 0d);
        assertEquals(1d, result.getFeatures().get("event_expose_count"), 0d);
        assertEquals(1d, result.getFeatures().get("event_click_count"), 0d);
    }

    @Test
    public void laterDeleteRetractsAndOlderMutationCannotResurrect() {
        Event event = event("u", "i", "s", "click", "2", "100");
        event.setEventId("event-1");
        EventFeatureAccumulator accumulator = new EventFeatureAccumulator();
        accumulator.add(FeatureUpdates.fromEvent(event, false, 10).get(0));
        FeatureSnapshot deleted = accumulator.add(
            FeatureUpdates.fromEvent(event, true, 20).get(0));
        assertEquals(0d, deleted.getFeatures().get("event_count"), 0d);
        FeatureSnapshot staleInsert = accumulator.add(
            FeatureUpdates.fromEvent(event, false, 15).get(0));
        assertEquals(0d, staleInsert.getFeatures().get("event_count"), 0d);
    }

    @Test
    public void windowsAndRecencyUseRequestedAsOfTime() {
        Event event = event("u", "i", "s", "click", "1", "100");
        EventFeatureAccumulator accumulator = new EventFeatureAccumulator();
        accumulator.add(FeatureUpdates.fromEvent(event).get(0));
        FeatureSnapshot result = accumulator.snapshot(100 + 86400 + 1);
        assertEquals(86401d, result.getFeatures().get("event_recency_seconds"), 0d);
        assertEquals(0d, result.getFeatures().get("event_count_1d"), 0d);
        assertEquals(1L, result.getRecentEventTimeCounts().get(100L).longValue());
    }

    @Test
    public void shortWindowsFilterExposureAndSumGenericEventValue() {
        EventFeatureAccumulator accumulator = new EventFeatureAccumulator();
        accumulator.add(FeatureUpdates.fromEvent(event("u", "i", "s", "expose", "2", "100")).get(0));
        accumulator.add(FeatureUpdates.fromEvent(event("u", "i", "s", "click", "3", "350")).get(0));
        FeatureSnapshot result = accumulator.snapshot(400);
        assertEquals(1d, result.getFeatures().get("event_expose_count_5m"), 0d);
        assertEquals(5d, result.getFeatures().get("event_value_sum_5m"), 0d);
    }

    @Test
    public void conversionRatesUseActionDenominatorsAndWindows() {
        EventFeatureAccumulator accumulator = new EventFeatureAccumulator();
        accumulator.add(FeatureUpdates.fromEvent(event("u", "i", "s", "expose", "1", "1")).get(0));
        accumulator.add(FeatureUpdates.fromEvent(event("u", "i", "s", "expose", "1", "2")).get(0));
        accumulator.add(FeatureUpdates.fromEvent(event("u", "i", "s", "click", "1", "3")).get(0));
        accumulator.add(FeatureUpdates.fromEvent(event("u", "i", "s", "collect", "1", "4")).get(0));
        accumulator.add(FeatureUpdates.fromEvent(event("u", "i", "s", "buy", "1", "5")).get(0));
        accumulator.add(FeatureUpdates.fromEvent(event("u", "i", "s", "expose", "1", "200000")).get(0));
        FeatureSnapshot result = accumulator.snapshot(200000);
        assertEquals(1d / 3d, result.getFeatures().get("event_ctr"), 0d);
        assertEquals(1d, result.getFeatures().get("event_collect_per_click"), 0d);
        assertEquals(1d, result.getFeatures().get("event_buy_per_click"), 0d);
        assertEquals(1d, result.getFeatures().get("event_buy_per_collect"), 0d);
        assertEquals(0d, result.getFeatures().get("event_ctr_1d"), 0d);
    }

    @Test
    public void materializesFrozenCategoryAndPriceContext() {
        Event click = event("u", "i1", "s", "click", "1", "100");
        context(click, "books", "fiction", 10d);
        Event buy = event("u", "i2", "s", "buy", "1", "200");
        context(buy, "music", "vinyl", 30d);
        EventFeatureAccumulator accumulator = new EventFeatureAccumulator();
        accumulator.add(FeatureUpdates.fromEvent(click).get(0));
        FeatureSnapshot result = accumulator.add(FeatureUpdates.fromEvent(buy).get(0));
        assertEquals("music,books", result.getStringFeatures().get("preferred_categories"));
        assertEquals("vinyl,fiction", result.getStringFeatures().get("preferred_subcategories"));
        assertEquals(20d, result.getFeatures().get("event_price_mean"), 0d);
        assertEquals(10d, result.getFeatures().get("event_price_std"), 0d);
        assertEquals(3d, result.getFeatures().get("event_buy_to_click_price_ratio"), 0d);
    }

    @Test
    public void productionSnapshotUsesInjectedWallClockAndPreservesInclusiveWindows() {
        long wallClock = 3000000L;
        EventFeatureAccumulator accumulator = new EventFeatureAccumulator();
        accumulator.add(FeatureUpdates.fromEvent(event(
            "u", "i30", "s", "expose", "1", Long.toString(wallClock - 30 * 86400L))).get(0));
        accumulator.add(FeatureUpdates.fromEvent(event(
            "u", "i7", "s", "click", "1", Long.toString(wallClock - 7 * 86400L))).get(0));
        accumulator.add(FeatureUpdates.fromEvent(event(
            "u", "i1", "s", "buy", "1", Long.toString(wallClock - 86400L))).get(0));

        FeatureSnapshot atBoundary = accumulator.currentSnapshot(wallClock);
        assertEquals(wallClock, atBoundary.getAsOfTime());
        assertEquals(86400d, atBoundary.getFeatures().get("event_recency_seconds"), 0d);
        assertEquals(1d, atBoundary.getFeatures().get("event_count_1d"), 0d);
        assertEquals(2d, atBoundary.getFeatures().get("event_count_7d"), 0d);
        assertEquals(3d, atBoundary.getFeatures().get("event_count_30d"), 0d);

        FeatureSnapshot afterBoundary = accumulator.currentSnapshot(wallClock + 1);
        assertEquals(0d, afterBoundary.getFeatures().get("event_count_1d"), 0d);
        assertEquals(1d, afterBoundary.getFeatures().get("event_count_7d"), 0d);
        assertEquals(2d, afterBoundary.getFeatures().get("event_count_30d"), 0d);
    }

    @Test
    public void productionSnapshotUsesFutureEventAsClockFloor() {
        EventFeatureAccumulator accumulator = new EventFeatureAccumulator();
        accumulator.add(FeatureUpdates.fromEvent(event(
            "u", "i", "s", "click", "1", "200")).get(0));

        FeatureSnapshot snapshot = accumulator.currentSnapshot(100);
        assertEquals(200L, snapshot.getAsOfTime());
        assertEquals(0d, snapshot.getFeatures().get("event_recency_seconds"), 0d);
        assertEquals(1d, snapshot.getFeatures().get("event_count_1d"), 0d);
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
            if (value.has("event_id")) {
                event.setEventId(value.get("event_id").getAsString());
            }
            event.setTraceId(value.get("trace_id").getAsString());
            boolean deleted = value.has("operation")
                && "DELETE".equals(value.get("operation").getAsString());
            long mutationTime = value.has("occurred_at")
                ? value.get("occurred_at").getAsLong() : 0;
            for (FeatureUpdate update : FeatureUpdates.fromEvent(event, deleted, mutationTime)) {
                if ("user".equals(update.getEntityType()) && update.getEventTime() <= asOf) {
                    accumulator.add(update);
                }
            }
        }
        FeatureSnapshot snapshot = accumulator.snapshot(asOf);
        JsonObject expected = fixture.getAsJsonObject("expected_user");
        for (String name : expected.keySet()) {
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

    private static void context(Event event, String category, String subcategory, double price) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("category", category); item.put("subcategory", subcategory); item.put("price", price);
        Map<String, Object> ext = new LinkedHashMap<>(); ext.put("_openrecItemContext", item);
        event.setExtFields(ext);
    }
}
