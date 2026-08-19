package com.openrec.dp.feature;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.openrec.proto.model.Event;

public final class FeatureUpdates {
    private FeatureUpdates() {}

    public static List<FeatureUpdate> fromEvent(Event event) {
        if (event == null || blank(event.getUserId()) || blank(event.getItemId())) {
            return Collections.emptyList();
        }
        long time = parseLong(event.getTime());
        double value = parseDouble(event.getValue());
        return Arrays.asList(
            update("user", event.getUserId(), event.getItemId(), event, time, value),
            update("item", event.getItemId(), event.getUserId(), event, time, value));
    }

    private static boolean blank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static FeatureUpdate update(String entityType, String entityId, String counterpart,
        Event event, long time, double value) {
        FeatureUpdate update = new FeatureUpdate();
        update.setEntityType(entityType);
        update.setEntityId(entityId);
        update.setCounterpartId(counterpart);
        update.setScene(event.getScene());
        update.setEventType(event.getType());
        update.setEventTime(time);
        update.setValue(value);
        return update;
    }

    private static long parseLong(String value) {
        try { return Long.parseLong(value); } catch (Exception ignored) { return 0L; }
    }

    private static double parseDouble(String value) {
        try { return Double.parseDouble(value); } catch (Exception ignored) { return 0d; }
    }
}
