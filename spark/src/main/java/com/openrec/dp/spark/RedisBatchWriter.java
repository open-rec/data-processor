package com.openrec.dp.spark;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

import com.openrec.dp.feature.FeatureJson;
import com.openrec.dp.feature.DislikeRules;
import com.openrec.dp.feature.EntityPartitions;
import com.openrec.dp.feature.EntityMessage;
import com.openrec.dp.feature.FeatureSnapshot;
import com.openrec.proto.model.Event;
import com.openrec.proto.model.Item;
import com.openrec.proto.model.User;

import redis.clients.jedis.JedisPooled;

final class RedisBatchWriter {
    private RedisBatchWriter() {}

    static StreamingQuery persistRaw(Dataset<Row> input, String type, Properties p) throws Exception {
        String output = p.getProperty("hdfs.output") + "/hive/" + type;
        String checkpoint = p.getProperty("checkpoint.path") + "/raw-" + type;
        String host = p.getProperty("redis.host"); int port = Integer.parseInt(p.getProperty("redis.port"));
        boolean hbaseEnabled = Boolean.parseBoolean(p.getProperty("hbase.enabled", "true"));
        boolean hiveEnabled = Boolean.parseBoolean(p.getProperty("hive.enabled", "true"));
        String quorum = p.getProperty("hbase.zookeeper.quorum", "zookeeper-1,zookeeper-2,zookeeper-3");
        String znode = p.getProperty("hbase.zookeeper.znode.parent", "/hbase");
        String prefix = p.getProperty("hbase.table.prefix", "openrec_");
        long newMaxItems = Long.parseLong(p.getProperty("redis.new.max-items", "10000"));
        return input.writeStream().option("checkpointLocation", checkpoint).foreachBatch((batch, id) -> {
            if (hiveEnabled) {
                batch.select(col("json"), udf((String json) -> EntityPartitions.bucket(type, json),
                    DataTypes.StringType).apply(col("json")).alias("bucket"))
                    .withColumn("dt", org.apache.spark.sql.functions.regexp_replace(col("bucket"), "^dt=", ""))
                    .drop("bucket").write().mode("append").partitionBy("dt").text(output);
            }
            batch.foreachPartition(rows -> {
                try (JedisPooled jedis = new JedisPooled(host, port);
                     HBaseEntityWriter hbase = hbaseEnabled
                         ? new HBaseEntityWriter(type, quorum, znode, prefix) : null) {
                    while (rows.hasNext()) {
                        String json = rows.next().getString(0);
                        writeRaw(jedis, type, json, newMaxItems);
                        if (hbase != null) { hbase.write(type, json); }
                    }
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

    private static void writeRaw(JedisPooled jedis, String type, String json, long newMaxItems) {
        EntityMessage message = EntityMessage.parse(type, json);
        if (message == null) { return; }
        json = message.getDataJson();
        if ("user".equals(type)) {
            User user = FeatureJson.fromJson(json, User.class);
            if (user != null && user.getId() != null) {
                if (message.isDelete()) { jedis.del("user:{" + user.getId() + "}"); }
                else { jedis.set("user:{" + user.getId() + "}", json); }
            }
        } else if ("item".equals(type)) {
            Item item = FeatureJson.fromJson(json, Item.class);
            if (item != null && item.getId() != null) {
                if (message.isDelete()) {
                    String existing = jedis.get("item:{" + item.getId() + "}");
                    Item old = FeatureJson.fromJson(existing, Item.class);
                    String scene = item.getScene() != null ? item.getScene()
                        : old == null ? null : old.getScene();
                    jedis.del("item:{" + item.getId() + "}");
                    if (scene != null) { jedis.zrem("new:{" + scene + "}", item.getId()); }
                    return;
                }
                jedis.set("item:{" + item.getId() + "}", json);
                if (item.getScene() != null && !item.getScene().trim().isEmpty()) {
                    String key = "new:{" + item.getScene() + "}";
                    jedis.zadd(key, number(item.getPubTime()), item.getId());
                    if (newMaxItems > 0) { jedis.zremrangeByRank(key, 0, -newMaxItems - 1); }
                }
            }
        } else {
            Event event = FeatureJson.fromJson(json, Event.class);
            if (event != null && event.getUserId() != null && event.getItemId() != null) {
                String key = "event:{" + event.getUserId() + "}:" + event.getScene() + ":" + event.getType();
                if ("dislike".equalsIgnoreCase(event.getType())) {
                    for (String rule : DislikeRules.parse(event.getValue())) {
                        jedis.zadd(key, number(event.getTime()), rule);
                    }
                } else {
                    jedis.zadd(key, number(event.getTime()), event.getItemId());
                }
            }
        }
    }
    private static double number(String value) {
        try { return Double.parseDouble(value); } catch (Exception ignored) { return 0d; }
    }
}
