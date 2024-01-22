package com.zdjizhi.function;

import com.zdjizhi.pojo.FileChunk;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

public class SideOutputMapFunction extends RichMapFunction<FileChunk, FileChunk> {

    private transient Counter pcapDelayedChunkCounter;
    private transient Counter trafficDelayedChunkCounter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
        pcapDelayedChunkCounter = metricGroup.counter("pcapDelayedChunkCount");
        trafficDelayedChunkCounter = metricGroup.counter("trafficDelayedChunkCount");
    }

    @Override
    public FileChunk map(FileChunk fileChunk) {
        fileChunk.setChunkCount(1);
        if ("seek".equals(fileChunk.getCombineMode())) {
            trafficDelayedChunkCounter.inc();
        } else {
            fileChunk.setChunkNumbers(fileChunk.getTimestamp() + "-" + fileChunk.getChunk().length + ";");
            pcapDelayedChunkCounter.inc();
        }
        return fileChunk;
    }

}
