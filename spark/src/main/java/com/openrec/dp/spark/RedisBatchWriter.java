package com.openrec.dp.spark;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;

import com.openrec.dp.feature.FeatureJson;
import com.openrec.dp.feature.FeatureSnapshot;
import com.openrec.proto.model.Event;
import com.openrec.proto.model.Item;
import com.openrec.proto.model.User;

import redis.clients.jedis.JedisPooled;

final class RedisBatchWriter {
    private RedisBatchWriter() {}

    static StreamingQuery persistRaw(Dataset<Row> input, String type, Properties p) throws Exception {
        String output = p.getProperty("hdfs.output") + "/raw/" + type;
        String checkpoint = p.getProperty("checkpoint.path") + "/raw-" + type;
        String host = p.getProperty("redis.host"); int port = Integer.parseInt(p.getProperty("redis.port"));
        return input.writeStream().option("checkpointLocation", checkpoint).foreachBatch((batch, id) -> {
            batch.select("json").write().mode("append").text(output);
            batch.foreachPartition(rows -> {
                try (JedisPooled jedis = new JedisPooled(host, port)) {
                    while (rows.hasNext()) { writeRaw(jedis, type, rows.next().getString(0)); }
                }
            });
        }).start();
    }

    static StreamingQuery persistSnapshots(Dataset<FeatureSnapshot> input, Properties p) throws Exception {
        String output = p.getProperty("hdfs.output") + "/features";
        String checkpoint = p.getProperty("checkpoint.path") + "/features";
        String host = p.getProperty("redis.host"); int port = Integer.parseInt(p.getProperty("redis.port"));
        return input.writeStream().outputMode("update").option("checkpointLocation", checkpoint)
            .foreachBatch((batch, id) -> {
                batch.toJSON().write().mode("append").text(output);
                batch.foreachPartition(snapshots -> {
                    try (JedisPooled jedis = new JedisPooled(host, port)) {
                        while (snapshots.hasNext()) {
                            FeatureSnapshot snapshot = snapshots.next();
                            jedis.set(snapshot.redisKey(), FeatureJson.toJson(snapshot));
                        }
                    }
                });
            }).start();
    }

    private static void writeRaw(JedisPooled jedis, String type, String json) {
        if ("user".equals(type)) {
            User user = FeatureJson.fromJson(json, User.class);
            if (user != null && user.getId() != null) { jedis.set("user:{" + user.getId() + "}", json); }
        } else if ("item".equals(type)) {
            Item item = FeatureJson.fromJson(json, Item.class);
            if (item != null && item.getId() != null) {
                jedis.set("item:{" + item.getId() + "}", json);
                jedis.zadd("new:{" + item.getScene() + "}", number(item.getPubTime()), item.getId());
            }
        } else {
            Event event = FeatureJson.fromJson(json, Event.class);
            if (event != null && event.getUserId() != null && event.getItemId() != null) {
                jedis.zadd("event:{" + event.getUserId() + "}:" + event.getScene() + ":" + event.getType(),
                    number(event.getTime()), event.getItemId());
            }
        }
    }
    private static double number(String value) {
        try { return Double.parseDouble(value); } catch (Exception ignored) { return 0d; }
    }
}
