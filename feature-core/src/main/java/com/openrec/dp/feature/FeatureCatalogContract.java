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
    private final Set<String> eventTypes;
    private final Set<String> userColumns;
    private final Set<String> itemColumns;

    private FeatureCatalogContract(int version, String sha256, List<Long> windows,
        Set<String> eventTypes, Set<String> userColumns, Set<String> itemColumns) {
        this.version = version; this.sha256 = sha256;
        this.windows = Collections.unmodifiableList(windows);
        this.eventTypes = Collections.unmodifiableSet(eventTypes);
        this.userColumns = Collections.unmodifiableSet(userColumns);
        this.itemColumns = Collections.unmodifiableSet(itemColumns);
    }

    public static FeatureCatalogContract get() { return INSTANCE; }
    public int getVersion() { return version; }
    public String getSha256() { return sha256; }
    public List<Long> getWindows() { return windows; }
    public Set<String> getEventTypes() { return eventTypes; }
    public Set<String> getColumns(String entity) {
        return "user".equals(entity) ? userColumns : itemColumns;
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
            Set<String> types = new LinkedHashSet<>();
            Set<String> users = new LinkedHashSet<>();
            Set<String> items = new LinkedHashSet<>();
            JsonArray features = root.getAsJsonArray("features");
            for (JsonElement element : features) {
                JsonObject feature = element.getAsJsonObject();
                if (!"behavior".equals(feature.get("group").getAsString())) { continue; }
                String entity = feature.get("entity").getAsString();
                String name = feature.get("name").getAsString();
                ("user".equals(entity) ? users : items).add(name);
                JsonObject aggregation = feature.getAsJsonObject("aggregation");
                if (aggregation != null && aggregation.has("window_seconds")) {
                    windows.add(aggregation.get("window_seconds").getAsLong());
                }
                if (aggregation != null && aggregation.has("filter")) {
                    types.add(aggregation.getAsJsonObject("filter").get("type").getAsString());
                }
            }
            return new FeatureCatalogContract(root.get("catalog_version").getAsInt(), hex(raw),
                new ArrayList<>(windows), types, users, items);
        } catch (Exception e) {
            throw new IllegalStateException("invalid feature catalog", e);
        }
    }

    private static String hex(byte[] raw) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(raw);
        StringBuilder result = new StringBuilder();
        for (byte value : digest) { result.append(String.format("%02x", value & 0xff)); }
        return result.toString();
    }
}
