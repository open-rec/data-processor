package com.openrec.dp.feature;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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
        values.put("event_unique_" + (("user".equals(entityType) || "session".equals(entityType))
            ? "item" : "user") + "_count",
            (double)counterparts.size());
        values.put("event_first_time", count == 0 ? 0d : (double)firstTime);
        values.put("event_last_time", (double)lastTime);
        values.put("event_recency_seconds", (double)Math.max(0L, asOfTime - lastTime));
        for (FeatureCatalogContract.WindowFeature feature : CATALOG.getWindowFeatures()) {
            double aggregate = 0d;
            long from = asOfTime - feature.getSeconds();
            for (FeatureUpdate update : events.values()) {
                if (update.getEventTime() < from || update.getEventTime() > asOfTime) { continue; }
                if (feature.getEventType() != null
                    && !feature.getEventType().equals(update.getEventType())) { continue; }
                aggregate += "sum".equals(feature.getOperator()) ? update.getValue() : 1d;
            }
            values.put(feature.getName(), aggregate);
        }
        for (String type : CATALOG.getEventTypes()) {
            values.put("event_" + type + "_count", (double)typeCounts.getOrDefault(type, 0L));
        }
        double clicks = values.get("event_click_count");
        double exposes = values.get("event_expose_count");
        values.put("event_click_rate", clicks + exposes == 0d
            ? 0d : clicks / (clicks + exposes));
        for (FeatureCatalogContract.RateFeature feature : CATALOG.getRateFeatures()) {
            double numerator = 0d;
            double denominator = 0d;
            long from = feature.getSeconds() == 0L
                ? Long.MIN_VALUE : asOfTime - feature.getSeconds();
            for (FeatureUpdate update : events.values()) {
                if (update.getEventTime() < from || update.getEventTime() > asOfTime) { continue; }
                if (feature.getNumeratorType().equals(update.getEventType())) { numerator++; }
                if (feature.getDenominatorType().equals(update.getEventType())) { denominator++; }
            }
            values.put(feature.getName(), denominator == 0d ? 0d : numerator / denominator);
        }
        commerce(values, result, asOfTime);
        if (!values.keySet().equals(CATALOG.getColumns(entityType))) {
            throw new IllegalStateException("feature implementation differs from catalog for " + entityType);
        }
        if (!result.getStringFeatures().keySet().equals(CATALOG.getStringColumns(entityType))) {
            throw new IllegalStateException("string feature implementation differs from catalog for " + entityType);
        }
        result.setFeatures(values);
        result.setRecentEventTimeCounts(new LinkedHashMap<>(timeCounts));
        return result;
    }

    private void commerce(Map<String, Double> values, FeatureSnapshot result, long asOfTime) {
        if (!"user".equals(entityType)) { return; }
        Map<String, Double> categories = new HashMap<>();
        Map<String, Double> subcategories = new HashMap<>();
        List<FeatureUpdate> priced = new ArrayList<>();
        for (FeatureUpdate update : events.values()) {
            double weight = behaviorWeight(update);
            addWeight(categories, update.getCategory(), weight);
            addWeight(subcategories, update.getSubcategory(), weight);
            if (update.isHasPrice()) { priced.add(update); }
        }
        Map<String, String> strings = new LinkedHashMap<>();
        strings.put("preferred_categories", top(categories));
        strings.put("preferred_subcategories", top(subcategories));
        result.setStringFeatures(strings);
        putPrice(values, "event_price", priced);
        for (String type : new String[] {"expose", "click", "collect", "buy"}) {
            putPrice(values, "event_" + type + "_price", select(priced, type, 0L, asOfTime));
        }
        for (long[] window : new long[][] {{86400L, 1L}, {604800L, 7L}, {2592000L, 30L}}) {
            for (String type : new String[] {"click", "buy"}) {
                putMean(values, "event_" + type + "_price_mean_" + window[1] + "d",
                    select(priced, type, asOfTime - window[0], asOfTime));
            }
        }
        values.put("event_buy_to_click_price_ratio", ratio(
            values.get("event_buy_price_mean"), values.get("event_click_price_mean")));
        values.put("event_recent_to_long_click_price_ratio", ratio(
            values.get("event_click_price_mean_1d"), values.get("event_click_price_mean_30d")));
    }

    private static double behaviorWeight(FeatureUpdate update) {
        String type = update.getEventType() == null ? "" : update.getEventType();
        if ("expose".equals(type)) { return 0.1d; }
        if ("click".equals(type)) { return 1d; }
        if ("collect".equals(type)) { return 3d; }
        if ("buy".equals(type)) { return 5d; }
        if ("stay".equals(type)) { return Math.log1p(Math.max(0d, update.getValue())); }
        return 0d;
    }

    private static void addWeight(Map<String, Double> target, String name, double weight) {
        if (name == null || name.trim().isEmpty() || weight <= 0d) { return; }
        target.put(name, target.getOrDefault(name, 0d) + weight);
    }

    private static String top(Map<String, Double> values) {
        List<Map.Entry<String, Double>> entries = new ArrayList<>(values.entrySet());
        entries.sort(Comparator.<Map.Entry<String, Double>, Double>comparing(Map.Entry::getValue)
            .reversed().thenComparing(Map.Entry::getKey));
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < Math.min(8, entries.size()); i++) {
            if (i > 0) { result.append(','); }
            result.append(entries.get(i).getKey());
        }
        return result.toString();
    }

    private static List<FeatureUpdate> select(List<FeatureUpdate> source, String type,
        long from, long to) {
        List<FeatureUpdate> result = new ArrayList<>();
        for (FeatureUpdate update : source) {
            if (type.equals(update.getEventType()) && update.getEventTime() >= from
                && update.getEventTime() <= to) { result.add(update); }
        }
        return result;
    }

    private static void putPrice(Map<String, Double> values, String prefix,
        List<FeatureUpdate> source) {
        putMean(values, prefix + "_mean", source);
        double mean = values.get(prefix + "_mean");
        double square = 0d;
        for (FeatureUpdate update : source) {
            double delta = update.getPrice() - mean; square += delta * delta;
        }
        values.put(prefix + "_std", source.isEmpty() ? 0d : Math.sqrt(square / source.size()));
    }

    private static void putMean(Map<String, Double> values, String name,
        List<FeatureUpdate> source) {
        double sum = 0d;
        for (FeatureUpdate update : source) { sum += update.getPrice(); }
        values.put(name, source.isEmpty() ? 0d : sum / source.size());
    }

    private static double ratio(Double numerator, Double denominator) {
        return numerator == null || denominator == null || denominator <= 0d
            ? 0d : numerator / denominator;
    }
}
