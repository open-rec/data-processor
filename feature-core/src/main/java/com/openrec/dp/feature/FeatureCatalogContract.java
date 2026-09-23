package com.openrec.dp.feature;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/** Runtime projection of the canonical model/feature/catalog contract. */
public final class FeatureCatalogContract {
    private static final FeatureCatalogContract INSTANCE = load();
    private final int version;
    private final String sha256;
    private final List<Long> windows;
    private final List<WindowFeature> windowFeatures;
    private final List<RateFeature> rateFeatures;
    private final Set<String> eventTypes;
    private final Set<String> userColumns;
    private final Set<String> itemColumns;
    private final Set<String> userStringColumns;
    private final Set<String> itemStringColumns;

    private FeatureCatalogContract(int version, String sha256, List<Long> windows,
        List<WindowFeature> windowFeatures, List<RateFeature> rateFeatures,
        Set<String> eventTypes, Set<String> userColumns, Set<String> itemColumns,
        Set<String> userStringColumns, Set<String> itemStringColumns) {
        this.version = version; this.sha256 = sha256;
        this.windows = Collections.unmodifiableList(windows);
        this.windowFeatures = Collections.unmodifiableList(windowFeatures);
        this.rateFeatures = Collections.unmodifiableList(rateFeatures);
        this.eventTypes = Collections.unmodifiableSet(eventTypes);
        this.userColumns = Collections.unmodifiableSet(userColumns);
        this.itemColumns = Collections.unmodifiableSet(itemColumns);
        this.userStringColumns = Collections.unmodifiableSet(userStringColumns);
        this.itemStringColumns = Collections.unmodifiableSet(itemStringColumns);
    }

    public static FeatureCatalogContract get() { return INSTANCE; }
    public int getVersion() { return version; }
    public String getSha256() { return sha256; }
    public List<Long> getWindows() { return windows; }
    public List<WindowFeature> getWindowFeatures() { return windowFeatures; }
    public List<RateFeature> getRateFeatures() { return rateFeatures; }
    public Set<String> getEventTypes() { return eventTypes; }
    public Set<String> getColumns(String entity) {
        return "user".equals(entity) ? userColumns : itemColumns;
    }
    public Set<String> getStringColumns(String entity) {
        return "user".equals(entity) ? userStringColumns : itemStringColumns;
    }

    private static FeatureCatalogContract load() {
        try (InputStream in = FeatureCatalogContract.class.getClassLoader()
                .getResourceAsStream("openrec-feature-catalog.json")) {
            if (in == null) { throw new IllegalStateException("feature catalog resource missing"); }
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096]; int read;
            while ((read = in.read(buffer)) >= 0) { output.write(buffer, 0, read); }
            byte[] raw = output.toByteArray();
            JsonObject root = new JsonParser().parse(new String(raw, "UTF-8")).getAsJsonObject();
            Set<Long> windows = new LinkedHashSet<>();
            List<WindowFeature> windowFeatures = new ArrayList<>();
            List<RateFeature> rateFeatures = new ArrayList<>();
            Set<String> types = new LinkedHashSet<>();
            Set<String> users = new LinkedHashSet<>();
            Set<String> items = new LinkedHashSet<>();
            Set<String> userStrings = new LinkedHashSet<>();
            Set<String> itemStrings = new LinkedHashSet<>();
            JsonArray features = root.getAsJsonArray("features");
            for (JsonElement element : features) {
                JsonObject feature = element.getAsJsonObject();
                if (!"behavior".equals(feature.get("group").getAsString())) { continue; }
                JsonObject materialization = feature.getAsJsonObject("materialization");
                if (materialization == null || !materialization.get("online").getAsBoolean()) {
                    continue;
                }
                String entity = feature.get("entity").getAsString();
                String name = feature.get("name").getAsString();
                boolean stringValue = "string".equals(feature.get("value_type").getAsString());
                if (stringValue) {
                    ("user".equals(entity) ? userStrings : itemStrings).add(name);
                } else {
                    ("user".equals(entity) ? users : items).add(name);
                }
                JsonObject aggregation = feature.getAsJsonObject("aggregation");
                boolean directEvent = "event".equals(
                    feature.getAsJsonObject("source").get("logical_entity").getAsString());
                if (directEvent && aggregation != null && aggregation.has("window_seconds")) {
                    long seconds = aggregation.get("window_seconds").getAsLong();
                    windows.add(seconds);
                    if ("user".equals(entity)
                        && !"ratio".equals(aggregation.get("operator").getAsString())) {
                        String filter = aggregation.has("filter")
                            ? aggregation.getAsJsonObject("filter").get("type").getAsString() : null;
                        windowFeatures.add(new WindowFeature(name, seconds,
                            aggregation.get("operator").getAsString(), filter));
                    }
                }
                if (directEvent && aggregation != null && "user".equals(entity)
                    && "ratio".equals(aggregation.get("operator").getAsString())) {
                    rateFeatures.add(new RateFeature(name,
                        aggregation.get("numerator_type").getAsString(),
                        aggregation.get("denominator_type").getAsString(),
                        aggregation.has("window_seconds")
                            ? aggregation.get("window_seconds").getAsLong() : 0L));
                }
                if (directEvent && aggregation != null && aggregation.has("filter")) {
                    types.add(aggregation.getAsJsonObject("filter").get("type").getAsString());
                }
            }
            return new FeatureCatalogContract(root.get("catalog_version").getAsInt(), hex(raw),
                new ArrayList<>(windows), windowFeatures, rateFeatures, types, users, items,
                userStrings, itemStrings);
        } catch (Exception e) {
            throw new IllegalStateException("invalid feature catalog", e);
        }
    }

    public static final class WindowFeature {
        private final String name;
        private final long seconds;
        private final String operator;
        private final String eventType;

        private WindowFeature(String name, long seconds, String operator, String eventType) {
            this.name = name; this.seconds = seconds; this.operator = operator;
            this.eventType = eventType;
        }
        public String getName() { return name; }
        public long getSeconds() { return seconds; }
        public String getOperator() { return operator; }
        public String getEventType() { return eventType; }
    }

    public static final class RateFeature {
        private final String name;
        private final String numeratorType;
        private final String denominatorType;
        private final long seconds;

        private RateFeature(String name, String numeratorType, String denominatorType,
            long seconds) {
            this.name = name; this.numeratorType = numeratorType;
            this.denominatorType = denominatorType; this.seconds = seconds;
        }
        public String getName() { return name; }
        public String getNumeratorType() { return numeratorType; }
        public String getDenominatorType() { return denominatorType; }
        public long getSeconds() { return seconds; }
    }

    private static String hex(byte[] raw) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(raw);
        StringBuilder result = new StringBuilder();
        for (byte value : digest) { result.append(String.format("%02x", value & 0xff)); }
        return result.toString();
    }
}
