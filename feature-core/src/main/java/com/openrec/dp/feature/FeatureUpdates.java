package com.openrec.dp.feature;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.openrec.proto.model.Event;

public final class FeatureUpdates {
    private FeatureUpdates() {}

    public static List<FeatureUpdate> fromEvent(Event event) {
        return fromEvent(event, false, 0L);
    }

    public static List<FeatureUpdate> fromEvent(Event event, boolean deleted, long mutationTime) {
        if (event == null || blank(event.getUserId()) || blank(event.getItemId())) {
            return Collections.emptyList();
        }
        Long time = parseLong(event.getTime());
        if (time == null) { return Collections.emptyList(); }
        double value = parseDouble(event.getValue());
        return Arrays.asList(
            update("user", event.getUserId(), event.getItemId(), event, time, value,
                deleted, mutationTime),
            update("item", event.getItemId(), event.getUserId(), event, time, value,
                deleted, mutationTime));
    }

    private static boolean blank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static FeatureUpdate update(String entityType, String entityId, String counterpart,
        Event event, long time, double value, boolean deleted, long mutationTime) {
        FeatureUpdate update = new FeatureUpdate();
        update.setEntityType(entityType);
        update.setEntityId(entityId);
        update.setCounterpartId(counterpart);
        update.setScene(event.getScene());
        update.setEventType(event.getType());
        update.setEventTime(time);
        update.setValue(value);
        update.setEventIdentity(identity(event, time));
        update.setDeleted(deleted);
        update.setMutationTime(mutationTime);
        return update;
    }

    private static Long parseLong(String value) {
        try {
            double parsed = Double.parseDouble(value);
            return Double.isFinite(parsed) && parsed == Math.rint(parsed) ? (long)parsed : null;
        } catch (Exception ignored) { return null; }
    }

    private static double parseDouble(String value) {
        try { return Double.parseDouble(value); } catch (Exception ignored) { return 0d; }
    }

    private static String identity(Event event, long time) {
        if (!blank(event.getEventId())) { return "event:" + event.getEventId(); }
        return "fields:" + event.getUserId() + "\u001f" + event.getItemId() + "\u001f"
            + text(event.getScene()) + "\u001f" + text(event.getType())
            + "\u001f" + time + "\u001f" + text(event.getTraceId());
    }

    public static String identity(Event event) {
        if (event == null) { return null; }
        Long time = parseLong(event.getTime());
        return time == null ? null : identity(event, time);
    }

    private static String text(String value) { return value == null ? "" : value; }
}
