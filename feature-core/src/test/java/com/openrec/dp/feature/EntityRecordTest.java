package com.openrec.dp.feature;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class EntityRecordTest {
    @Test public void extractsStableEntityKeys() {
        assertEquals("u1", EntityRecord.fromJson("user", "{\"id\":\"u1\"}").getRowKey());
        assertEquals("i1", EntityRecord.fromJson("item", "{\"id\":\"i1\"}").getRowKey());
        assertEquals("t1", EntityRecord.fromJson("event",
            "{\"traceId\":\"t1\",\"userId\":\"u1\",\"itemId\":\"i1\"}").getRowKey());
        assertEquals("10#u1#i1#home#click", EntityRecord.fromJson("event",
            "{\"time\":\"10\",\"userId\":\"u1\",\"itemId\":\"i1\",\"scene\":\"home\",\"type\":\"click\"}").getRowKey());
    }

    @Test public void rejectsUnknownOrIncompleteEntities() {
        assertNull(EntityRecord.fromJson("user", "{}"));
        assertNull(EntityRecord.fromJson("event", "{\"userId\":\"u1\"}"));
        assertNull(EntityRecord.fromJson("unknown", "{}"));
    }

    @Test public void parsesVersionedDeleteEnvelope() {
        String value = "{\"schemaVersion\":1,\"entityType\":\"item\","
            + "\"operation\":\"DELETE\",\"occurredAt\":1700000000000,\"data\":{\"id\":\"i1\"}}";
        EntityMessage message = EntityMessage.parse("item", value);
        assertEquals(true, message.isDelete());
        assertEquals("i1", EntityRecord.fromJson("item", value).getRowKey());
    }
}
