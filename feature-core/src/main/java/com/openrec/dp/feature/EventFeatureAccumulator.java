package com.openrec.dp.feature;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Shared event feature formula used by both Flink and Spark. */
public class EventFeatureAccumulator implements Serializable {
    private static final long DAY = 86400L;
    private static final int[] WINDOWS = {1, 7, 30};
    private static final Set<String> TYPES = new HashSet<>(
        Arrays.asList("click", "expose", "buy", "collect", "stay"));

    private String entityType;
    private String entityId;
    private long count;
    private double valueSum;
    private long firstTime = Long.MAX_VALUE;
    private long lastTime;
    private Set<Long> activeDays = new HashSet<>();
    private Set<String> scenes = new HashSet<>();
    private Set<String> counterparts = new HashSet<>();
    private Map<String, Long> typeCounts = new HashMap<>();
    private TreeMap<Long, Long> timeCounts = new TreeMap<>();

    public FeatureSnapshot add(FeatureUpdate update) {
        if (entityId == null) {
            entityType = update.getEntityType();
            entityId = update.getEntityId();
        }
        count++;
        valueSum += update.getValue();
        firstTime = Math.min(firstTime, update.getEventTime());
        lastTime = Math.max(lastTime, update.getEventTime());
        activeDays.add(update.getEventTime() / DAY);
        if (update.getScene() != null) { scenes.add(update.getScene()); }
        if (update.getCounterpartId() != null) { counterparts.add(update.getCounterpartId()); }
        String type = update.getEventType() == null ? "" : update.getEventType();
        if (TYPES.contains(type)) { typeCounts.put(type, typeCounts.getOrDefault(type, 0L) + 1); }
        timeCounts.put(update.getEventTime(), timeCounts.getOrDefault(update.getEventTime(), 0L) + 1);
        // Only the largest online window needs individual timestamps. All-time totals remain above.
        timeCounts.headMap(lastTime - 30L * DAY, false).clear();
        return snapshot(lastTime);
    }

    public FeatureSnapshot snapshot(long asOfTime) {
        FeatureSnapshot result = new FeatureSnapshot();
        result.setEntityType(entityType);
        result.setEntityId(entityId);
        result.setAsOfTime(asOfTime);
        Map<String, Double> values = new LinkedHashMap<>();
        values.put("event_count", (double)count);
        values.put("event_value_sum", valueSum);
        values.put("event_value_mean", count == 0 ? 0d : valueSum / count);
        values.put("event_active_days", (double)activeDays.size());
        values.put("event_unique_scene_count", (double)scenes.size());
        values.put("event_unique_" + ("user".equals(entityType) ? "item" : "user") + "_count",
            (double)counterparts.size());
        values.put("event_first_time", count == 0 ? 0d : (double)firstTime);
        values.put("event_last_time", (double)lastTime);
        values.put("event_recency_seconds", (double)Math.max(0L, asOfTime - lastTime));
        for (int days : WINDOWS) {
            long from = asOfTime - days * DAY;
            long windowCount = timeCounts.tailMap(from, true).values().stream().mapToLong(Long::longValue).sum();
            values.put("event_count_" + days + "d", (double)windowCount);
        }
        for (String type : Arrays.asList("click", "expose", "buy", "collect", "stay")) {
            values.put("event_" + type + "_count", (double)typeCounts.getOrDefault(type, 0L));
        }
        double clicks = values.get("event_click_count");
        double exposes = values.get("event_expose_count");
        values.put("event_click_rate", clicks + exposes == 0 ? 0d : clicks / (clicks + exposes));
        result.setFeatures(values);
        return result;
    }
}
