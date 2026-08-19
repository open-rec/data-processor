package com.openrec.dp.feature;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class FeatureSnapshot implements Serializable {
    private String entityType;
    private String entityId;
    private long asOfTime;
    private Map<String, Double> features = new LinkedHashMap<>();

    public String redisKey() { return "feature:" + entityType + ":{" + entityId + "}"; }
    public String getEntityType() { return entityType; }
    public void setEntityType(String value) { this.entityType = value; }
    public String getEntityId() { return entityId; }
    public void setEntityId(String value) { this.entityId = value; }
    public long getAsOfTime() { return asOfTime; }
    public void setAsOfTime(long value) { this.asOfTime = value; }
    public Map<String, Double> getFeatures() { return features; }
    public void setFeatures(Map<String, Double> value) { this.features = value; }
}
