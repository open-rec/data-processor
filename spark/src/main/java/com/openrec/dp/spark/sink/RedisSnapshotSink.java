package com.openrec.dp.spark.sink;

import java.util.Properties;

import org.apache.spark.sql.Dataset;

import com.openrec.dp.feature.FeatureJson;
import com.openrec.dp.feature.FeatureSnapshot;

import redis.clients.jedis.JedisPooled;

public final class RedisSnapshotSink {
    private RedisSnapshotSink() {}

    public static void write(Dataset<FeatureSnapshot> batch, Properties p) {
        String host = p.getProperty("redis.host");
        int port = Integer.parseInt(p.getProperty("redis.port"));
        batch.foreachPartition(snapshots -> {
            try (JedisPooled jedis = new JedisPooled(host, port)) {
                while (snapshots.hasNext()) {
                    FeatureSnapshot snapshot = snapshots.next();
                    jedis.set(snapshot.redisKey(), FeatureJson.toJson(snapshot));
                }
            }
        });
    }
}
