package com.openrec.dp.feature;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.openrec.dp.feature.process.ServingProjection;

public class ServingProjectionTest {
    @Test
    public void userDeleteAndRejectedMutationDoNotResurrectServingData() {
        MemoryStore store = new MemoryStore();
        write(store, "user", "{\"id\":\"u\"}");
        assertNotNull(store.values.get("user:{u}"));
        write(store, "user", envelope("user", "{\"id\":\"u\"}"));
        assertFalse(store.values.containsKey("user:{u}"));
        assertEquals("mutation:user:{u}:200:true", store.fences.get(1));
        store.accept = false;
        write(store, "user", "{\"id\":\"u\"}");
        assertFalse(store.values.containsKey("user:{u}"));
    }

    @Test
    public void itemDeleteUsesStoredSceneAndNewItemsRemainBounded() {
        MemoryStore store = new MemoryStore();
        write(store, "item", "{\"id\":\"i\",\"scene\":\"home\",\"pubTime\":\"42\"}");
        assertEquals(42d, store.scores.get("new:{home}").get("i"), 0d);
        assertEquals("new:{home}:0:-11", store.trim);
        write(store, "item", envelope("item", "{\"id\":\"i\"}"));
        assertFalse(store.values.containsKey("item:{i}"));
        assertFalse(store.scores.get("new:{home}").containsKey("i"));
    }

    @Test
    public void eventsKeepServingKeyAndMutationIdentity() {
        MemoryStore store = new MemoryStore();
        String event = "{\"eventId\":\"e\",\"userId\":\"u\",\"itemId\":\"i\","
            + "\"scene\":\"home\",\"type\":\"click\",\"time\":\"50\"}";
        write(store, "event", event);
        assertEquals(50d, store.scores.get("event:{u}:home:click").get("i"), 0d);
        write(store, "event", envelope("event", event));
        assertTrue(store.scores.get("event:{u}:home:click").isEmpty());
        assertEquals("mutation:event:{event:e}:200:true", store.fences.get(1));
    }

    @Test
    public void dislikeExpandsAndRetractsRules() {
        MemoryStore store = new MemoryStore();
        String event = "{\"eventId\":\"e\",\"userId\":\"u\",\"itemId\":\"i\","
            + "\"scene\":\"home\",\"type\":\"dislike\",\"time\":\"50\","
            + "\"value\":\"{\\\"category\\\":\\\"news\\\"}\"}";
        write(store, "event", event);
        assertEquals(50d, store.scores.get("event:{u}:home:dislike").get("category:news"), 0d);
        write(store, "event", envelope("event", event));
        assertTrue(store.scores.get("event:{u}:home:dislike").isEmpty());
    }

    @Test
    public void invalidAndMismatchedMessagesDoNotWrite() {
        MemoryStore store = new MemoryStore();
        write(store, "user", "bad json");
        write(store, "user", "{}");
        write(store, "user", envelope("item", "{\"id\":\"i\"}"));
        assertTrue(store.values.isEmpty());
        assertTrue(store.fences.isEmpty());
    }

    private static void write(MemoryStore store, String type, String json) {
        ServingProjection.write(store, type, json, 10);
    }

    private static String envelope(String type, String data) {
        return "{\"schemaVersion\":1,\"entityType\":\"" + type
            + "\",\"operation\":\"DELETE\",\"occurredAt\":200,\"data\":" + data + "}";
    }

    /** Records storage calls; fencing itself remains the Redis adapter's responsibility. */
    private static class MemoryStore implements ServingProjection.Store {
        final Map<String, String> values = new HashMap<>();
        final Map<String, Map<String, Double>> scores = new HashMap<>();
        final List<String> fences = new ArrayList<>();
        boolean accept = true;
        String trim;

        public boolean acceptMutation(String key, long occurredAt, boolean deleted) {
            fences.add(key + ":" + occurredAt + ":" + deleted);
            return accept;
        }

        public String get(String key) {
            return values.get(key);
        }

        public void set(String key, String value) {
            values.put(key, value);
        }

        public void del(String key) {
            values.remove(key);
        }

        public void zadd(String key, double score, String member) {
            scores.computeIfAbsent(key, ignored -> new HashMap<>()).put(member, score);
        }

        public void zrem(String key, String member) {
            scores.computeIfAbsent(key, ignored -> new HashMap<>()).remove(member);
        }

        public void zremrangeByRank(String key, long start, long stop) {
            trim = key + ":" + start + ":" + stop;
        }
    }
}
