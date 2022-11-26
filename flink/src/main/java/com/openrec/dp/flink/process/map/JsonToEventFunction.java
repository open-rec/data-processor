package com.openrec.dp.flink.process.map;

import com.openrec.dp.flink.util.JsonUtil;
import com.openrec.proto.model.Event;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class JsonToEventFunction implements MapFunction<String, Event> {

    @Override
    public Event map(String s) throws Exception {
        Event event = null;
        if(!StringUtils.isEmpty(s)) {
            event = JsonUtil.jsonToObj(s, Event.class);
        }
        return event;
    }
}
