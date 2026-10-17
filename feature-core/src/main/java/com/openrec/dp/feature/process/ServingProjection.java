package com.openrec.dp.feature.process;

import com.openrec.dp.feature.FeatureJson;
import com.openrec.dp.feature.DislikeRules;
import com.openrec.dp.feature.EntityMessage;
import com.openrec.dp.feature.FeatureUpdates;
import com.openrec.proto.model.Event;
import com.openrec.proto.model.Item;
import com.openrec.proto.model.User;

/** Shared entity projection rules. The storage adapter owns connections and mutation fencing. */
public final class ServingProjection {
    private ServingProjection() {}

    public interface Store {
        boolean acceptMutation(String key, long occurredAt, boolean deleted);

        String get(String key);

        void set(String key, String value);

        void del(String key);

        void zadd(String key, double score, String member);

        void zrem(String key, String member);

        void zremrangeByRank(String key, long start, long stop);
    }

    public static void write(Store store, String type, String json, long newMaxItems) {
        EntityMessage message = EntityMessage.parse(type, json);
        if (message == null) {
            return;
        }
        json = message.getDataJson();
        if ("user".equals(type)) {
            User user = FeatureJson.fromJson(json, User.class);
            if (user != null && user.getId() != null) {
                if (!store.acceptMutation("mutation:user:{" + user.getId() + "}", message.getOccurredAt(),
                    message.isDelete())) {
                    return;
                }
                if (message.isDelete()) {
                    store.del("user:{" + user.getId() + "}");
                } else {
                    store.set("user:{" + user.getId() + "}", json);
                }
            }
        } else if ("item".equals(type)) {
            Item item = FeatureJson.fromJson(json, Item.class);
            if (item != null && item.getId() != null) {
                if (!store.acceptMutation("mutation:item:{" + item.getId() + "}", message.getOccurredAt(),
                    message.isDelete())) {
                    return;
                }
                if (message.isDelete()) {
                    String existing = store.get("item:{" + item.getId() + "}");
                    Item old = FeatureJson.fromJson(existing, Item.class);
                    String scene = item.getScene() != null ? item.getScene() : old == null ? null : old.getScene();
                    store.del("item:{" + item.getId() + "}");
                    if (scene != null) {
                        store.zrem("new:{" + scene + "}", item.getId());
                    }
                    return;
                }
                store.set("item:{" + item.getId() + "}", json);
                if (item.getScene() != null && !item.getScene().trim().isEmpty()) {
                    String key = "new:{" + item.getScene() + "}";
                    store.zadd(key, number(item.getPubTime()), item.getId());
                    if (newMaxItems > 0) {
                        store.zremrangeByRank(key, 0, -newMaxItems - 1);
                    }
                }
            }
        } else {
            Event event = FeatureJson.fromJson(json, Event.class);
            if (event != null && event.getUserId() != null && event.getItemId() != null) {
                String identity = FeatureUpdates.identity(event);
                if (identity == null || !store.acceptMutation("mutation:event:{" + identity + "}",
                    message.getOccurredAt(), message.isDelete())) {
                    return;
                }
                String key = "event:{" + event.getUserId() + "}:" + event.getScene() + ":" + event.getType();
                if ("dislike".equalsIgnoreCase(event.getType())) {
                    for (String rule : DislikeRules.parse(event.getValue())) {
                        if (message.isDelete()) {
                            store.zrem(key, rule);
                        } else {
                            store.zadd(key, number(event.getTime()), rule);
                        }
                    }
                } else {
                    if (message.isDelete()) {
                        store.zrem(key, event.getItemId());
                    } else {
                        store.zadd(key, number(event.getTime()), event.getItemId());
                    }
                }
            }
        }
    }

    private static double number(String value) {
        try {
            return Double.parseDouble(value);
        } catch (Exception ignored) {
            return 0d;
        }
    }
}
