package com.openrec.dp.feature;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import com.openrec.proto.model.Event;
import com.openrec.proto.model.Item;
import com.openrec.proto.model.User;

/** Derives the UTC Hive day partition from an entity's business timestamp. */
public final class EntityPartitions {
    private static final DateTimeFormatter DAY = DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneOffset.UTC);
    private EntityPartitions() { }

    public static String bucket(String type, String json) {
        return bucket(type, json, System.currentTimeMillis());
    }

    static String bucket(String type, String json, long fallbackMillis) {
        EntityMessage message = EntityMessage.parse(type, json);
        if (message == null) { return "dt=" + DAY.format(Instant.ofEpochMilli(fallbackMillis)); }
        json = message.getDataJson();
        if (message.isDelete() && message.getOccurredAt() > 0) {
            fallbackMillis = message.getOccurredAt();
        }
        String timestamp = null;
        if ("event".equals(type)) {
            Event value = FeatureJson.fromJson(json, Event.class);
            if (value != null) { timestamp = value.getTime(); }
        } else if ("item".equals(type)) {
            Item value = FeatureJson.fromJson(json, Item.class);
            if (value != null) { timestamp = first(value.getModifyTime(), value.getPubTime()); }
        } else if ("user".equals(type)) {
            User value = FeatureJson.fromJson(json, User.class);
            if (value != null) { timestamp = first(value.getLoginTime(), value.getRegisterTime()); }
        }
        return "dt=" + DAY.format(Instant.ofEpochMilli(epochMillis(timestamp, fallbackMillis)));
    }

    private static String first(String preferred, String fallback) {
        return preferred == null || preferred.trim().isEmpty() ? fallback : preferred;
    }

    private static long epochMillis(String value, long fallback) {
        try {
            long parsed = Long.parseLong(value);
            return parsed < 100000000000L ? parsed * 1000L : parsed;
        } catch (Exception ignored) {
            return fallback;
        }
    }
}
