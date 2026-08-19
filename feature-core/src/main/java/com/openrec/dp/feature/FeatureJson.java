package com.openrec.dp.feature;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public final class FeatureJson {
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
    private FeatureJson() {}
    public static String toJson(Object value) { return GSON.toJson(value); }
    public static <T> T fromJson(String value, Class<T> type) {
        if (value == null) { return null; }
        try { return GSON.fromJson(value, type); }
        catch (RuntimeException ignored) { return null; }
    }
}
