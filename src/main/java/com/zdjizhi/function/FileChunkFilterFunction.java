package com.zdjizhi.function;

import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.pojo.FileChunk;
import org.apache.commons.jexl3.*;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;

public class FileChunkFilterFunction extends RichFilterFunction<FileChunk> {
    private static final Log LOG = LogFactory.get();

    private final String filterExpression;
    private final String functionName;
    public transient Counter filterChunksCounter;
    public transient Counter emlChunksCounter;
    public transient Counter txtChunksCounter;
    private transient JexlExpression jexlExpression;
    private transient JexlContext jexlContext;

    public FileChunkFilterFunction(String filterExpression, String functionName) {
        this.filterExpression = filterExpression;
        this.functionName = functionName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("file_chunk_combiner", "filter_" + functionName);
        filterChunksCounter = metricGroup.counter("filterChunksCount");
        emlChunksCounter = metricGroup.counter("emlChunksCount");
        txtChunksCounter = metricGroup.counter("txtChunksCount");
        metricGroup.meter("numEmlChunksInPerSecond", new MeterView(emlChunksCounter));
        metricGroup.meter("numTxtChunksInPerSecond", new MeterView(txtChunksCounter));
        metricGroup.meter("numChunksFilterPerSecond", new MeterView(filterChunksCounter));
        JexlEngine jexlEngine = new JexlBuilder().create();
        jexlExpression = jexlEngine.createExpression(filterExpression);
        jexlContext = new MapContext();
    }

    @Override
    public boolean filter(FileChunk value) {
        if (value == null) {
            filterChunksCounter.inc();
            return false;
        }
        if (CharSequenceUtil.isNotEmpty(filterExpression)) {
            jexlContext.set("FileChunk", value);
            if (!Boolean.parseBoolean(jexlExpression.evaluate(jexlContext).toString())) {
                filterChunksCounter.inc();
                String fileType = value.getFileType();
                if ("eml".equals(fileType)) {
                    emlChunksCounter.inc();
                } else if ("txt".equals(fileType)) {
                    txtChunksCounter.inc();
                }
                LOG.info(functionName + " filter: " + value.toString());
                return false;
            }
        }
        return true;
    }
}
