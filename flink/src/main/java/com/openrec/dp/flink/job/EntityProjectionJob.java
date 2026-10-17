package com.openrec.dp.flink.job;

import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;

import com.openrec.dp.flink.sink.RawRedisSink;
import com.openrec.dp.flink.sink.HBaseEntitySink;
import com.openrec.dp.flink.sink.HistoryFileSink;

public final class EntityProjectionJob {
    private EntityProjectionJob() {}

    public static void attach(DataStream<String> stream, String type, Properties p) {
        stream.addSink(new RawRedisSink(type, p)).name(type + "-serving-redis");
        if (Boolean.parseBoolean(p.getProperty("hbase.enabled", "true"))) {
            stream.addSink(new HBaseEntitySink(type, p)).name(type + "-entity-hbase");
        }
        if (Boolean.parseBoolean(p.getProperty("hive.enabled", "true"))) {
            stream.sinkTo(HistoryFileSink.entityFileSink(p, type));
        }
    }
}
