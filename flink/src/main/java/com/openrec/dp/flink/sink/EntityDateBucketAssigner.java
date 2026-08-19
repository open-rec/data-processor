package com.openrec.dp.flink.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import com.openrec.dp.feature.EntityPartitions;

public class EntityDateBucketAssigner implements BucketAssigner<String, String> {
    private final String type;
    public EntityDateBucketAssigner(String type) { this.type = type; }
    @Override public String getBucketId(String json, Context context) {
        return EntityPartitions.bucket(type, json);
    }
    @Override public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
