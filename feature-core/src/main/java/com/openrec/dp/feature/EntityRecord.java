package com.openrec.dp.feature;

import java.io.Serializable;

import com.openrec.proto.model.Event;
import com.openrec.proto.model.Item;
import com.openrec.proto.model.User;

/** A validated raw entity together with its stable HBase row key. */
public class EntityRecord implements Serializable {
    private final String type;
    private final String rowKey;
    private final String json;

    public EntityRecord(String type, String rowKey, String json) {
        this.type = type;
        this.rowKey = rowKey;
        this.json = json;
    }

    public String getType() { return type; }
    public String getRowKey() { return rowKey; }
    public String getJson() { return json; }

    public static EntityRecord fromJson(String type, String json) {
        if (blank(type) || blank(json)) { return null; }
        EntityMessage message = EntityMessage.parse(type, json);
        if (message == null) { return null; }
        json = message.getDataJson();
        if ("user".equals(type)) {
            User user = FeatureJson.fromJson(json, User.class);
            return user == null || blank(user.getId()) ? null : new EntityRecord(type, user.getId(), json);
        }
        if ("item".equals(type)) {
            Item item = FeatureJson.fromJson(json, Item.class);
            return item == null || blank(item.getId()) ? null : new EntityRecord(type, item.getId(), json);
        }
        if ("event".equals(type)) {
            Event event = FeatureJson.fromJson(json, Event.class);
            if (event == null || blank(event.getUserId()) || blank(event.getItemId())) { return null; }
            String key = blank(event.getTraceId())
                ? part(event.getTime()) + "#" + event.getUserId() + "#" + event.getItemId()
                    + "#" + part(event.getScene()) + "#" + part(event.getType())
                : event.getTraceId();
            return new EntityRecord(type, key, json);
        }
        return null;
    }

    private static String part(String value) { return value == null ? "" : value; }
    private static boolean blank(String value) { return value == null || value.trim().isEmpty(); }
}
