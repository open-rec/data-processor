package com.openrec.dp.flink.process;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.openrec.dp.feature.EventFeatureAccumulator;
import com.openrec.dp.feature.FeatureSnapshot;
import com.openrec.dp.feature.FeatureUpdate;

public class EventFeatureProcessFunction extends KeyedProcessFunction<String, FeatureUpdate, FeatureSnapshot> {
    private transient ValueState<EventFeatureAccumulator> state;
    @Override public void open(Configuration parameters) {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>(
            "event-feature-accumulator", EventFeatureAccumulator.class));
    }
    @Override public void processElement(FeatureUpdate value, Context context,
        Collector<FeatureSnapshot> out) throws Exception {
        EventFeatureAccumulator accumulator = state.value();
        if (accumulator == null) { accumulator = new EventFeatureAccumulator(); }
        out.collect(accumulator.add(value));
        state.update(accumulator);
    }
}
