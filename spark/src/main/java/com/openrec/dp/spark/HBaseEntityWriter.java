package com.openrec.dp.spark;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import com.openrec.dp.feature.EntityRecord;

final class HBaseEntityWriter implements Closeable {
    private static final byte[] FAMILY = "entity".getBytes(StandardCharsets.UTF_8);
    private static final byte[] JSON = "json".getBytes(StandardCharsets.UTF_8);
    private final Connection connection;
    private final Table table;

    HBaseEntityWriter(String type, String quorum, String znode, String prefix) throws Exception {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", quorum);
        config.set("zookeeper.znode.parent", znode);
        connection = ConnectionFactory.createConnection(config);
        TableName name = TableName.valueOf(prefix + type);
        try (Admin admin = connection.getAdmin()) {
            if (!admin.tableExists(name)) {
                try {
                    admin.createTable(TableDescriptorBuilder.newBuilder(name)
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build());
                } catch (org.apache.hadoop.hbase.TableExistsException ignored) { }
            }
        }
        table = connection.getTable(name);
    }

    void write(String type, String json) throws Exception {
        EntityRecord record = EntityRecord.fromJson(type, json);
        if (record == null) { return; }
        Put put = new Put(record.getRowKey().getBytes(StandardCharsets.UTF_8));
        put.addColumn(FAMILY, JSON, record.getJson().getBytes(StandardCharsets.UTF_8));
        table.put(put);
    }

    @Override public void close() {
        try { table.close(); } catch (Exception ignored) { }
        try { connection.close(); } catch (Exception ignored) { }
    }
}
