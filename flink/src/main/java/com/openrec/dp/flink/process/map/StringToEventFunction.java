package com.openrec.dp.flink.process.map;

import com.openrec.proto.model.Event;
import org.apache.flink.api.common.functions.MapFunction;

public class StringToEventFunction implements MapFunction<String, Event> {

    @Override
    public Event map(String s) throws Exception {
        Event event = null;
        String[] ss=s.split(",");
        if(ss.length==10) {
            event = new Event();
            event.setUserId(ss[1]);
            event.setItemId(ss[2]);
            event.setTraceId(ss[3]);
            event.setScene(ss[4]);
            event.setType(ss[5]);
            event.setValue(ss[6]);
            event.setTime(ss[7]);
            event.setLogin(Boolean.parseBoolean(ss[8]));
            event.setExtFields(ss[9]);
        }
        return event;
    }
}
