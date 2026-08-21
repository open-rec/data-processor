package com.openrec.dp.flink.sink;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;

import com.openrec.dp.feature.EntityRecord;
import com.openrec.dp.feature.EntityMessage;

/** Persists the unmodified Kafka JSON as an HBase entity value. */
public class HBaseEntitySink extends RichSinkFunction<String> {
    private static final byte[] FAMILY = "entity".getBytes(StandardCharsets.UTF_8);
    private static final byte[] JSON = "json".getBytes(StandardCharsets.UTF_8);
    private final String type;
    private final String quorum;
    private final String znode;
    private final String tablePrefix;
    private transient Connection connection;
    private transient Table table;

    public HBaseEntitySink(String type, Properties p) {
        this.type = type;
        quorum = p.getProperty("hbase.zookeeper.quorum", "zookeeper-1,zookeeper-2,zookeeper-3");
        znode = p.getProperty("hbase.zookeeper.znode.parent", "/hbase");
        tablePrefix = p.getProperty("hbase.table.prefix", "openrec_");
    }

    @Override public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", quorum);
        config.set("zookeeper.znode.parent", znode);
        connection = ConnectionFactory.createConnection(config);
        TableName name = TableName.valueOf(tablePrefix + type);
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

    @Override public void invoke(String json, Context context) throws Exception {
        EntityRecord record = EntityRecord.fromJson(type, json);
        if (record == null) { return; }
        EntityMessage message = EntityMessage.parse(type, json);
        if (message != null && message.isDelete()) {
            table.delete(new Delete(record.getRowKey().getBytes(StandardCharsets.UTF_8)));
            return;
        }
        Put put = new Put(record.getRowKey().getBytes(StandardCharsets.UTF_8));
        put.addColumn(FAMILY, JSON, record.getJson().getBytes(StandardCharsets.UTF_8));
        table.put(put);
    }

    @Override public void close() throws Exception {
        if (table != null) { table.close(); }
        if (connection != null) { connection.close(); }
    }
}
