package com.openrec.dp.feature;

import java.io.Serializable;

public class FeatureUpdate implements Serializable {
    private String entityType;
    private String entityId;
    private String counterpartId;
    private String scene;
    private String eventType;
    private double value;
    private long eventTime;

    public String key() { return entityType + ":" + entityId; }
    public String getEntityType() { return entityType; }
    public void setEntityType(String value) { this.entityType = value; }
    public String getEntityId() { return entityId; }
    public void setEntityId(String value) { this.entityId = value; }
    public String getCounterpartId() { return counterpartId; }
    public void setCounterpartId(String value) { this.counterpartId = value; }
    public String getScene() { return scene; }
    public void setScene(String value) { this.scene = value; }
    public String getEventType() { return eventType; }
    public void setEventType(String value) { this.eventType = value; }
    public double getValue() { return value; }
    public void setValue(double value) { this.value = value; }
    public long getEventTime() { return eventTime; }
    public void setEventTime(long value) { this.eventTime = value; }
}
