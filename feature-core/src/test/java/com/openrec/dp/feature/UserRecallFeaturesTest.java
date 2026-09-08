package com.openrec.dp.feature;

import java.util.Arrays;
import com.openrec.proto.model.User;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class UserRecallFeaturesTest {
    @Test public void normalizesProfileAttributesForUserRecall() {
        User user = new User();
        user.setCity("Hangzhou"); user.setGender("Female"); user.setTags(Arrays.asList("Film", "Music"));
        assertEquals(Arrays.asList("city:hangzhou", "gender:female", "tags:film", "tags:music"),
            UserRecallFeatures.attributeTokens(user));
    }
}
