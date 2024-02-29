package com.zdjizhi.function;

import cn.hutool.core.util.StrUtil;
import com.zdjizhi.pojo.FileChunk;
import org.apache.commons.jexl3.*;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

public class FileChunkFilter extends RichFilterFunction<FileChunk> {
    private final long maxFileSize;
    private final String filterExpression;
    private transient Counter filterChunkCounter;
    private JexlExpression jexlExpression;
    private JexlContext jexlContext;

    public FileChunkFilter(long maxFileSize, String filterExpression) {
        this.maxFileSize = maxFileSize;
        this.filterExpression = filterExpression;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
        filterChunkCounter = metricGroup.counter("filterChunkCount");
        JexlEngine jexlEngine = new JexlBuilder().create();
        jexlExpression = jexlEngine.createExpression(filterExpression);
        jexlContext = new MapContext();
    }

    @Override
    public boolean filter(FileChunk value) {
        if (value == null || value.getOffset() > maxFileSize) {
            filterChunkCounter.inc();
            return false;
        }
        if (StrUtil.isNotEmpty(filterExpression)) {
            jexlContext.set(value.getClass().getSimpleName(), value);
            if (!Boolean.parseBoolean(jexlExpression.evaluate(jexlContext).toString())) {
                filterChunkCounter.inc();
                return false;
            }
        }
        return true;
    }
}
