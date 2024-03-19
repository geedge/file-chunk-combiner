package com.zdjizhi.function;

import com.zdjizhi.pojo.FileChunk;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;

import static com.zdjizhi.utils.PublicConstants.COMBINE_MODE_APPEND;

public class SideOutputMapFunction extends RichMapFunction<FileChunk, FileChunk> {

    public transient Counter delayedChunksCounter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
        delayedChunksCounter = metricGroup.counter("delayedChunksCount");
        metricGroup.meter("numChunksDelayPerSecond", new MeterView(delayedChunksCounter));
    }

    @Override
    public FileChunk map(FileChunk fileChunk) {
        delayedChunksCounter.inc();
        fileChunk.setChunkCount(1);
        if (COMBINE_MODE_APPEND.equals(fileChunk.getCombineMode())) {
            fileChunk.setChunkNumbers(fileChunk.getTimestamp() + "-" + fileChunk.getChunk().length + ";");
        }
        return fileChunk;
    }
}
