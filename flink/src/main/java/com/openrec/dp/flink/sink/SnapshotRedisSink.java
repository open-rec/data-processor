package com.openrec.dp.flink.sink;

import java.util.Properties;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.openrec.dp.feature.FeatureJson;
import com.openrec.dp.feature.FeatureSnapshot;

import redis.clients.jedis.JedisPooled;

public class SnapshotRedisSink extends RichSinkFunction<FeatureSnapshot> {
    private final String host;
    private final int port;
    private transient JedisPooled jedis;
    public SnapshotRedisSink(Properties p) {
        host = p.getProperty("redis.host"); port = Integer.parseInt(p.getProperty("redis.port"));
    }
    @Override public void open(Configuration parameters) { jedis = new JedisPooled(host, port); }
    @Override public void invoke(FeatureSnapshot value, Context context) {
        jedis.set(value.redisKey(), FeatureJson.toJson(value));
    }
    @Override public void close() { if (jedis != null) { jedis.close(); } }
}
