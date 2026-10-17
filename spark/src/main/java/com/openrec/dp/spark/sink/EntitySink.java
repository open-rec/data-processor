package com.openrec.dp.spark.sink;

import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import redis.clients.jedis.JedisPooled;

/** Fan-out inside one micro-batch, preserving history-before-serving write order. */
public final class EntitySink {
    private EntitySink() {}

    public static void writeBatch(Dataset<Row> batch, String type, Properties p) {
        String output = p.getProperty("hdfs.output") + "/hive/" + type;
        String host = p.getProperty("redis.host");
        int port = Integer.parseInt(p.getProperty("redis.port"));
        boolean hbaseEnabled = Boolean.parseBoolean(p.getProperty("hbase.enabled", "true"));
        boolean hiveEnabled = Boolean.parseBoolean(p.getProperty("hive.enabled", "true"));
        String quorum = p.getProperty("hbase.zookeeper.quorum", "zookeeper-1,zookeeper-2,zookeeper-3");
        String znode = p.getProperty("hbase.zookeeper.znode.parent", "/hbase");
        String prefix = p.getProperty("hbase.table.prefix", "openrec_");
        long newMaxItems = Long.parseLong(p.getProperty("redis.new.max-items", "10000"));
        if (hiveEnabled) {
            HistorySink.writeEntities(batch, type, output);
        }
        batch.foreachPartition(rows -> {
            try (JedisPooled jedis = new JedisPooled(host, port);
                HBaseEntityWriter hbase = hbaseEnabled ? new HBaseEntityWriter(type, quorum, znode, prefix) : null) {
                while (rows.hasNext()) {
                    String json = rows.next().getString(0);
                    RedisEntitySink.write(jedis, type, json, newMaxItems);
                    if (hbase != null) {
                        hbase.write(type, json);
                    }
                }
            }
        });
    }
}
