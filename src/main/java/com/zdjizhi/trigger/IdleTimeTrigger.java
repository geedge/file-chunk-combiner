package com.zdjizhi.trigger;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class IdleTimeTrigger<W extends TimeWindow> extends Trigger<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private final long maxIdleTime;

    private IdleTimeTrigger(long maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
    }

    public static <W extends TimeWindow> IdleTimeTrigger<TimeWindow> of(long maxIdleTime) {
        return new IdleTimeTrigger<>(maxIdleTime);
    }

    private final ReducingStateDescriptor<Long> processingTimeStateDesc =
            new ReducingStateDescriptor<>("processTimer", new ReduceMax(), LongSerializer.INSTANCE);

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(processingTimeStateDesc);
        fireTimestamp.clear();
        long nextFireTimestamp = ctx.getCurrentProcessingTime() + maxIdleTime;
        ctx.registerProcessingTimeTimer(nextFireTimestamp);
        fireTimestamp.add(nextFireTimestamp);
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(processingTimeStateDesc);
        if (fireTimestamp.get() != null && fireTimestamp.get() == time) {
            fireTimestamp.clear();
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) {
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(processingTimeStateDesc);
        fireTimestamp.clear();
    }

    private static class ReduceMax implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) {
            return Math.max(value1, value2);
        }
    }
}
