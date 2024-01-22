package com.zdjizhi.sink;

import com.zdjizhi.pojo.FileChunk;
import com.zdjizhi.utils.HttpClientUtil;
import com.zdjizhi.utils.PublicUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.IOException;

public class HosSink extends RichSinkFunction<FileChunk> {

    private final Configuration configuration;
    private transient Counter sendHosErrorCounter;

    public HosSink(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
        sendHosErrorCounter = metricGroup.counter("sendHosErrorCount");
    }

    @Override
    public void invoke(FileChunk fileChunk, Context context) {
        PublicUtil.sendToHos(fileChunk, configuration, sendHosErrorCounter);
    }

    @Override
    public void close() throws IOException {
        HttpClientUtil.getInstance(null).close();
    }

}
