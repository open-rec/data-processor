package com.openrec.dp.spark.sink;

import java.util.Arrays;
import java.util.Collections;

import com.openrec.dp.feature.process.ServingProjection;

import redis.clients.jedis.JedisPooled;

public final class RedisEntitySink {
    private RedisEntitySink() {}

    public static void write(JedisPooled jedis, String type, String json, long newMaxItems) {
        ServingProjection.write(new ServingProjection.Store() {
            public boolean acceptMutation(String key, long occurredAt, boolean deleted) {
                return RedisEntitySink.acceptMutation(jedis, key, occurredAt, deleted);
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
}
