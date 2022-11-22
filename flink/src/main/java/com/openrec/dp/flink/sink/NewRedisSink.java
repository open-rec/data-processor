package com.openrec.dp.flink.sink;

import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class NewRedisSink extends RedisSink {

    static class NewRedisMapper implements RedisMapper {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return null;
        }

        @Override
        public String getKeyFromData(Object o) {
            return null;
        }

        @Override
        public String getValueFromData(Object o) {
            return null;
        }
    }


    public NewRedisSink(FlinkJedisConfigBase flinkJedisConfigBase) {
        super(flinkJedisConfigBase, new NewRedisMapper());
    }
}
