package com.openrec.dp.spark.process;

import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.KeyValueGroupedDataset;
import scala.Tuple2;

import com.openrec.dp.feature.EventFeatureAccumulator;
import com.openrec.dp.feature.EntityMessage;
import com.openrec.dp.feature.FeatureJson;
import com.openrec.dp.feature.FeatureSnapshot;
import com.openrec.dp.feature.FeatureUpdate;
import com.openrec.dp.feature.FeatureUpdates;
import com.openrec.proto.model.Event;

public final class FeatureAggregationProcess {
    private FeatureAggregationProcess() {}

    public static Dataset<FeatureUpdate> updates(Dataset<Row> events) {
        return events.flatMap((FlatMapFunction<Row, FeatureUpdate>)row -> {
            EntityMessage message = EntityMessage.parse("event", row.getString(0));
            Event event = message == null ? null : FeatureJson.fromJson(message.getDataJson(), Event.class);
            return event == null ? java.util.Collections.emptyIterator()
                : FeatureUpdates.fromEvent(event, message.isDelete(), message.getOccurredAt()).iterator();
        }, Encoders.kryo(FeatureUpdate.class));
    }

    public static Dataset<Tuple2<String, String>> rebuildState(Dataset<FeatureUpdate> updates) {
        return updates.groupByKey((MapFunction<FeatureUpdate, String>)FeatureUpdate::key, Encoders.STRING())
            .mapGroups((MapGroupsFunction<String, FeatureUpdate, Tuple2<String, String>>)(key, values) -> {
                EventFeatureAccumulator accumulator = new EventFeatureAccumulator();
                while (values.hasNext()) {
                    accumulator.add(values.next());
                }
                return new Tuple2<>(key, FeatureJson.toJson(accumulator));
            }, Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
    }

    public static Iterator<FeatureSnapshot> aggregate(String key, Iterator<FeatureUpdate> values,
        GroupState<String> state) {
        EventFeatureAccumulator accumulator = state.exists()
            ? FeatureJson.fromJson(state.get(), EventFeatureAccumulator.class) : new EventFeatureAccumulator();
        if (accumulator == null) {
            throw new IllegalStateException("Invalid feature JSON state for " + key);
        }
        FeatureSnapshot latest = null;
        while (values.hasNext()) {
            accumulator.add(values.next());
            latest = accumulator.currentSnapshot();
        }
        state.update(FeatureJson.toJson(accumulator));
        return latest == null ? java.util.Collections.emptyIterator()
            : java.util.Collections.singletonList(latest).iterator();
    }

    public static FeatureSnapshot aggregateForParity(Iterable<FeatureUpdate> values, long asOfTime) {
        EventFeatureAccumulator accumulator = new EventFeatureAccumulator();
        for (FeatureUpdate value : values) {
            accumulator.add(value);
        }
        return accumulator.snapshot(asOfTime);
    }

    public static Dataset<FeatureSnapshot> aggregateBatchForParity(Dataset<FeatureUpdate> updates,
        final long asOfTime) {
        KeyValueGroupedDataset<String, FeatureUpdate> grouped =
            updates.groupByKey((MapFunction<FeatureUpdate, String>)FeatureUpdate::key, Encoders.STRING());
        return grouped.mapGroups((MapGroupsFunction<String, FeatureUpdate, FeatureSnapshot>)(key, values) -> {
            EventFeatureAccumulator accumulator = new EventFeatureAccumulator();
            while (values.hasNext()) {
                accumulator.add(values.next());
            }
            return accumulator.snapshot(asOfTime);
        }, Encoders.bean(FeatureSnapshot.class));
    }
}
