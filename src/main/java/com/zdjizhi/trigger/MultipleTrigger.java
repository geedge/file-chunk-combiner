package com.zdjizhi.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.List;

public class MultipleTrigger<T, W extends Window> extends Trigger<T, W> {
    private static final long serialVersionUID = 1L;

    private final List<Trigger<T, W>> triggers;

    private MultipleTrigger(List<Trigger<T, W>> triggers) {
        this.triggers = triggers;
    }

    public static <T, W extends Window> MultipleTrigger<T, W> of(List<Trigger<T, W>> triggers) {
        return new MultipleTrigger<>(triggers);
    }

    @Override
    public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception {
        TriggerResult result = TriggerResult.CONTINUE;
        for (Trigger<T, W> trigger : triggers) {
            TriggerResult triggerResult = trigger.onElement(element, timestamp, window, ctx);
            if (triggerResult == TriggerResult.FIRE) {
                result = TriggerResult.FIRE_AND_PURGE;
                break;
            }
        }
        return result;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        TriggerResult result = TriggerResult.CONTINUE;
        for (Trigger<T, W> trigger : triggers) {
            TriggerResult triggerResult = trigger.onProcessingTime(time, window, ctx);
            if (triggerResult == TriggerResult.FIRE) {
                result = TriggerResult.FIRE_AND_PURGE;
                break;
            }
        }
        return result;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        TriggerResult result = TriggerResult.CONTINUE;
        for (Trigger<T, W> trigger : triggers) {
            TriggerResult triggerResult = trigger.onEventTime(time, window, ctx);
            if (triggerResult == TriggerResult.FIRE) {
                result = TriggerResult.FIRE_AND_PURGE;
                break;
            }
        }
        return result;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        for (Trigger<T, W> trigger : triggers) {
            trigger.clear(window, ctx);
        }
    }
}
