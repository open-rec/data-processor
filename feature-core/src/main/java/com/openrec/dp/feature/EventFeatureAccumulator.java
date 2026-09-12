package com.openrec.dp.feature;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Shared event feature formula used by both Flink and Spark. */
public class EventFeatureAccumulator implements Serializable {
    private static final long DAY = 86400L;
    private static final FeatureCatalogContract CATALOG = FeatureCatalogContract.get();

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
    private Map<String, FeatureUpdate> events = new HashMap<>();
    private Map<String, Long> mutationTimes = new HashMap<>();

    public FeatureSnapshot add(FeatureUpdate update) {
        String identity = update.getEventIdentity();
        long previousMutation = mutationTimes.getOrDefault(identity, Long.MIN_VALUE);
        if (identity == null || update.getMutationTime() < previousMutation) {
            return snapshot(lastTime);
        }
        if (entityId == null) {
            entityType = update.getEntityType();
            entityId = update.getEntityId();
        }
        // Equal mutation times are idempotent. A DELETE wins a tie so replay order cannot resurrect.
        if (update.getMutationTime() == previousMutation && !update.isDeleted()) {
            return snapshot(lastTime);
        }
        mutationTimes.put(identity, update.getMutationTime());
        FeatureUpdate previous = events.get(identity);
        if (update.isDeleted()) {
            events.remove(identity);
            if (previous != null) { rebuild(); }
        } else {
            events.put(identity, update);
            if (previous == null) { addContribution(update); }
            else { rebuild(); }
        }
        return snapshot(lastTime);
    }

    private void rebuild() {
        count = 0L; valueSum = 0d; firstTime = Long.MAX_VALUE; lastTime = 0L;
        activeDays.clear(); scenes.clear(); counterparts.clear(); typeCounts.clear();
        timeCounts.clear();
        for (FeatureUpdate update : events.values()) {
            addContribution(update);
        }
    }

    private void addContribution(FeatureUpdate update) {
        count++;
        valueSum += update.getValue();
        firstTime = Math.min(firstTime, update.getEventTime());
        lastTime = Math.max(lastTime, update.getEventTime());
        activeDays.add(update.getEventTime() / DAY);
        if (update.getScene() != null && !update.getScene().trim().isEmpty()) {
            scenes.add(update.getScene());
        }
        if (update.getCounterpartId() != null) { counterparts.add(update.getCounterpartId()); }
        String type = update.getEventType() == null ? "" : update.getEventType();
        if (CATALOG.getEventTypes().contains(type)) {
            typeCounts.put(type, typeCounts.getOrDefault(type, 0L) + 1);
        }
        timeCounts.put(update.getEventTime(), timeCounts.getOrDefault(
            update.getEventTime(), 0L) + 1);
        if (count > 0) {
            long largestWindow = Collections.max(CATALOG.getWindows());
            timeCounts.headMap(lastTime - largestWindow, false).clear();
        }
    }

    /** Materialize at wall-clock time for an online serving snapshot. */
    public FeatureSnapshot currentSnapshot() {
        return currentSnapshot(System.currentTimeMillis() / 1000L);
    }

    /** Deterministic production adapter for tests and callers with an authoritative clock. */
    public FeatureSnapshot currentSnapshot(long wallClockEpochSeconds) {
        return snapshot(Math.max(lastTime, wallClockEpochSeconds));
    }

    public FeatureSnapshot snapshot(long asOfTime) {
        FeatureSnapshot result = new FeatureSnapshot();
        result.setEntityType(entityType);
        result.setEntityId(entityId);
        result.setAsOfTime(asOfTime);
        result.setSourceWatermark(lastTime);
        result.setCatalogVersion(CATALOG.getVersion());
        result.setCatalogSha256(CATALOG.getSha256());
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
        for (long seconds : CATALOG.getWindows()) {
            long from = asOfTime - seconds;
            long windowCount = timeCounts.tailMap(from, true).values().stream().mapToLong(Long::longValue).sum();
            values.put("event_count_" + (seconds / DAY) + "d", (double)windowCount);
        }
        for (String type : CATALOG.getEventTypes()) {
            values.put("event_" + type + "_count", (double)typeCounts.getOrDefault(type, 0L));
        }
        double clicks = values.get("event_click_count");
        double exposes = values.get("event_expose_count");
        values.put("event_click_rate", clicks + exposes == 0 ? 0d : clicks / (clicks + exposes));
        if (!values.keySet().equals(CATALOG.getColumns(entityType))) {
            throw new IllegalStateException("feature implementation differs from catalog for " + entityType);
        }
        result.setFeatures(values);
        result.setRecentEventTimeCounts(new LinkedHashMap<>(timeCounts));
        return result;
    }
}
