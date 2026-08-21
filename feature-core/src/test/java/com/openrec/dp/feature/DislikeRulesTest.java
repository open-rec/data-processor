package com.openrec.dp.feature;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.LinkedHashSet;

import org.junit.Test;

public class DislikeRulesTest {
    @Test
    public void expandsStructuredValueForRedis() {
        assertEquals(new LinkedHashSet<>(Arrays.asList("id:i1", "category:c1", "tag:t1", "tag:t2")),
            DislikeRules.parse("{\"id\":\"i1\",\"category\":\"c1\",\"tags\":[\"t1\",\"t2\"]}"));
    }
}
