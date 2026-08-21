package com.openrec.dp.feature;

import java.util.LinkedHashSet;
import java.util.Set;

import com.openrec.proto.model.DislikeValue;

/** Materializes the structured dislike value as Redis-filterable members. */
public final class DislikeRules {
    private DislikeRules() {}

    public static Set<String> parse(String value) {
        DislikeValue dislike = FeatureJson.fromJson(value, DislikeValue.class);
        Set<String> rules = new LinkedHashSet<>();
        add(rules, "id:", dislike == null ? null : dislike.getId());
        add(rules, "category:", dislike == null ? null : dislike.getCategory());
        if (dislike != null && dislike.getTags() != null) {
            for (String tag : dislike.getTags()) { add(rules, "tag:", tag); }
        }
        return rules;
    }

    private static void add(Set<String> rules, String prefix, String value) {
        if (value != null && !value.trim().isEmpty()) { rules.add(prefix + value.trim()); }
    }
}
