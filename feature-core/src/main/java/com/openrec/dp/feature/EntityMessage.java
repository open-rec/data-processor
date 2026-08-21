package com.openrec.dp.feature;

import java.io.Serializable;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.openrec.proto.biz.push.PushCmd;

/** Parses the v1 mutation envelope while retaining legacy bare-entity compatibility. */
public class EntityMessage implements Serializable {
    private final String entityType;
    private final PushCmd operation;
    private final long occurredAt;
    private final String dataJson;

    private EntityMessage(String entityType, PushCmd operation, long occurredAt, String dataJson) {
        this.entityType = entityType; this.operation = operation;
        this.occurredAt = occurredAt; this.dataJson = dataJson;
    }

    public String getEntityType() { return entityType; }
    public PushCmd getOperation() { return operation; }
    public long getOccurredAt() { return occurredAt; }
    public String getDataJson() { return dataJson; }
    public boolean isDelete() { return operation == PushCmd.DELETE; }

    public static EntityMessage parse(String expectedType, String json) {
        if (json == null) { return null; }
        try {
            // Spark carries an older Gson where the static parseString helper is unavailable.
            JsonElement parsed = new JsonParser().parse(json);
            if (!parsed.isJsonObject()) { return null; }
            JsonObject object = parsed.getAsJsonObject();
            if (!object.has("schemaVersion")) {
                return new EntityMessage(expectedType, PushCmd.INSERT, 0L, json);
            }
            String type = object.get("entityType").getAsString();
            if (!expectedType.equals(type) || !object.has("data")) { return null; }
            PushCmd operation = PushCmd.valueOf(object.get("operation").getAsString());
            long occurredAt = object.has("occurredAt") ? object.get("occurredAt").getAsLong() : 0L;
            return new EntityMessage(type, operation, occurredAt, object.get("data").toString());
        } catch (RuntimeException ignored) { return null; }
    }
}
