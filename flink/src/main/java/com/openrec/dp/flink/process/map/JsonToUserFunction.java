package com.openrec.dp.flink.process.map;

import com.openrec.dp.flink.util.JsonUtil;
import com.openrec.proto.model.User;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class JsonToUserFunction implements MapFunction<String, User> {

    @Override
    public User map(String s) throws Exception {
        User user = null;
        if (!StringUtils.isEmpty(s)) {
            user = JsonUtil.jsonToObj(s, User.class);
        }
        return user;
    }
}
