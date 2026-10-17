package com.openrec.dp.flink.sink;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;

public final class HistoryFileSink {
    private HistoryFileSink() {}

    public static FileSink<String> fileSink(Properties p, String path) {
        return FileSink
            .forRowFormat(new Path(p.getProperty("hdfs.output") + "/" + path), new SimpleStringEncoder<String>("UTF-8"))
            .build();
    }

    public static FileSink<String> entityFileSink(Properties p, String type) {
        return FileSink
            .forRowFormat(new Path(p.getProperty("hdfs.output") + "/hive/" + type),
                new SimpleStringEncoder<String>("UTF-8"))
            .withBucketAssigner(new EntityDateBucketAssigner(type)).build();
    }
}
