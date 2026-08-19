package com.openrec.dp.feature;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class EntityPartitionsTest {
    @Test public void derivesUtcBusinessDateForEveryEntity() {
        assertEquals("dt=2023-11-14", EntityPartitions.bucket("event", "{\"time\":\"1700000000\"}", 0));
        assertEquals("dt=2023-11-14", EntityPartitions.bucket("item", "{\"pubTime\":\"1700000000000\"}", 0));
        assertEquals("dt=2023-11-14", EntityPartitions.bucket("user", "{\"registerTime\":\"1700000000\"}", 0));
    }

    @Test public void usesFallbackDateForMissingTimestamp() {
        assertEquals("dt=1970-01-01", EntityPartitions.bucket("event", "{}", 0));
    }
}
