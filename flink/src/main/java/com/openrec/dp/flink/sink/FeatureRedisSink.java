package com.openrec.dp.flink.sink;

import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class FeatureRedisSink extends RedisSink {

    public FeatureRedisSink(FlinkJedisConfigBase flinkJedisConfigBase) {
        super(flinkJedisConfigBase, new FeatureRedisMapper());
    }

    static class FeatureRedisMapper implements RedisMapper {

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
}
