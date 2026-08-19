package com.openrec.dp.flink.sink;

import java.util.Properties;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.openrec.dp.feature.FeatureJson;
import com.openrec.proto.model.Event;
import com.openrec.proto.model.Item;
import com.openrec.proto.model.User;

import redis.clients.jedis.JedisPooled;

/** Maintains the Redis serving projection while HDFS keeps the immutable training input. */
public class RawRedisSink extends RichSinkFunction<String> {
    private final String type;
    private final String host;
    private final int port;
    private final long newMaxItems;
    private transient JedisPooled jedis;
    public RawRedisSink(String type, Properties p) {
        this.type = type; host = p.getProperty("redis.host");
        port = Integer.parseInt(p.getProperty("redis.port"));
        newMaxItems = Long.parseLong(p.getProperty("redis.new.max-items", "10000"));
    }
    @Override public void open(Configuration parameters) { jedis = new JedisPooled(host, port); }
    @Override public void invoke(String json, Context context) {
        if ("user".equals(type)) {
            User user = FeatureJson.fromJson(json, User.class);
            if (user != null && user.getId() != null) { jedis.set("user:{" + user.getId() + "}", json); }
        } else if ("item".equals(type)) {
            Item item = FeatureJson.fromJson(json, Item.class);
            if (item != null && item.getId() != null) {
                jedis.set("item:{" + item.getId() + "}", json);
                if (item.getScene() != null && !item.getScene().trim().isEmpty()) {
                    String key = "new:{" + item.getScene() + "}";
                    jedis.zadd(key, parse(item.getPubTime()), item.getId());
                    if (newMaxItems > 0) { jedis.zremrangeByRank(key, 0, -newMaxItems - 1); }
                }
            }
        } else {
            Event event = FeatureJson.fromJson(json, Event.class);
            if (event != null && event.getUserId() != null && event.getItemId() != null) {
                jedis.zadd("event:{" + event.getUserId() + "}:" + event.getScene() + ":" + event.getType(),
                    parse(event.getTime()), event.getItemId());
            }
        }
    }
    private static double parse(String value) {
        try { return Double.parseDouble(value); } catch (Exception ignored) { return 0d; }
    }
    @Override public void close() { if (jedis != null) { jedis.close(); } }
}
