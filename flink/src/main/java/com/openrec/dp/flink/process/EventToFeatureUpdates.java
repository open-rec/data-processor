package com.openrec.dp.flink.process;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import com.openrec.dp.feature.FeatureJson;
import com.openrec.dp.feature.FeatureUpdate;
import com.openrec.dp.feature.FeatureUpdates;
import com.openrec.proto.model.Event;

public class EventToFeatureUpdates implements FlatMapFunction<String, FeatureUpdate> {
    @Override public void flatMap(String json, Collector<FeatureUpdate> out) {
        Event event = FeatureJson.fromJson(json, Event.class);
        FeatureUpdates.fromEvent(event).forEach(out::collect);
    }
}
