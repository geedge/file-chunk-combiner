package com.zdjizhi.function;

import com.zdjizhi.config.Configs;
import com.zdjizhi.pojo.FileChunk;
import com.zdjizhi.utils.PublicUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class CombineChunkProcessWindowFunction extends ProcessWindowFunction<FileChunk, FileChunk, String, TimeWindow> {

    private transient Counter duplicateChunkCounter;
    public transient Counter combineErrorCounter;
    public transient Counter seekChunkCounter;
    public transient Counter appendChunkCounter;
    private final Configuration configuration;

    public CombineChunkProcessWindowFunction(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
        duplicateChunkCounter = metricGroup.counter("duplicateChunkCount");
        combineErrorCounter = metricGroup.counter("combineErrorCount");
        seekChunkCounter = metricGroup.counter("seekChunkCount");
        appendChunkCounter = metricGroup.counter("appendChunkCount");
    }

    @Override
    public void process(String string, Context context, Iterable<FileChunk> elements, Collector<FileChunk> out) {
        List<FileChunk> fileChunks = PublicUtil.combine(elements, configuration.get(Configs.FILE_MAX_CHUNK_COUNT), duplicateChunkCounter, combineErrorCounter, seekChunkCounter, appendChunkCounter);
        for (FileChunk fileChunk : fileChunks) {
            out.collect(fileChunk);
        }
    }
}
