package com.openrec.dp.flink.sink;

import java.util.Properties;
import java.util.Arrays;
import java.util.Collections;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;

import com.openrec.dp.feature.process.ServingProjection;

import redis.clients.jedis.JedisPooled;

/** Maintains the Redis serving projection while HDFS keeps the immutable training input. */
public class RawRedisSink extends RichSinkFunction<String> {
    private final String type;
    private final String host;
    private final int port;
    private final long newMaxItems;
    private transient JedisPooled jedis;

    public RawRedisSink(String type, Properties p) {
        this.type = type;
        host = p.getProperty("redis.host");
        port = Integer.parseInt(p.getProperty("redis.port"));
        newMaxItems = Long.parseLong(p.getProperty("redis.new.max-items", "10000"));
    }

    @Override
    public void open(OpenContext parameters) {
        jedis = new JedisPooled(host, port);
    }

    @Override
    public void invoke(String json, Context context) {
        ServingProjection.write(new ServingProjection.Store() {
            public boolean acceptMutation(String key, long occurredAt, boolean deleted) {
                return RawRedisSink.acceptMutation(jedis, key, occurredAt, deleted);
            }

            public String get(String key) {
                return jedis.get(key);
            }

            public void set(String key, String value) {
                jedis.set(key, value);
            }

            public void del(String key) {
                jedis.del(key);
            }

            public void zadd(String key, double score, String member) {
                jedis.zadd(key, score, member);
            }

            public void zrem(String key, String member) {
                jedis.zrem(key, member);
            }

            public void zremrangeByRank(String key, long start, long stop) {
                jedis.zremrangeByRank(key, start, stop);
            }
        }, type, json, newMaxItems);
    }

    private static boolean acceptMutation(JedisPooled jedis, String key, long occurredAt, boolean deleted) {
        String script =
            "local old=redis.call('GET',KEYS[1]); " + "local incoming=tonumber(ARGV[1]); local tomb=tonumber(ARGV[2]); "
                + "if old then local sep=string.find(old,':'); local ts=tonumber(string.sub(old,1,sep-1)); "
                + "local was_tomb=tonumber(string.sub(old,sep+1)); "
                + "if incoming < ts or (incoming == ts and tomb < was_tomb) then return 0 end end; "
                + "redis.call('SET',KEYS[1],ARGV[1]..':'..ARGV[2]); return 1";
        Object result = jedis.eval(script, Collections.singletonList(key),
            Arrays.asList(Long.toString(occurredAt), deleted ? "1" : "0"));
        return Long.valueOf(1L).equals(result);
    }

    @Override
    public void close() {
        if (jedis != null) {
            jedis.close();
        }
    }
}
