package com.openrec.dp.feature;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import com.openrec.proto.model.User;

/** Canonical profile tokens shared with attribute-based user recall. */
public final class UserRecallFeatures {
    private UserRecallFeatures() { }

    public static List<String> attributeTokens(User user) {
        if (user == null) return Collections.emptyList();
        List<String> result = new ArrayList<>();
        add(result, "gender", user.getGender());
        add(result, "country", user.getCountry());
        add(result, "city", user.getCity());
        if (user.getTags() != null) for (String tag : user.getTags()) add(result, "tags", tag);
        Collections.sort(result);
        return result;
    }

    private static void add(List<String> target, String name, String value) {
        if (value == null) return;
        for (String part : value.toLowerCase(Locale.ROOT).split("[,|/\\s]+")) {
            if (!part.trim().isEmpty()) target.add(name + ":" + part.trim());
        }
    }
}
