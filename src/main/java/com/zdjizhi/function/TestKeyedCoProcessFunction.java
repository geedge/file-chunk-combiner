package com.zdjizhi.function;

import cn.hutool.core.io.IoUtil;
import com.zdjizhi.config.Configs;
import com.zdjizhi.pojo.FileChunk;
import com.zdjizhi.utils.HBaseConnectionUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestKeyedCoProcessFunction extends KeyedCoProcessFunction<String, FileChunk, FileChunk, FileChunk> {

    private final Configuration configuration;
    public transient Counter chunksInCounter;
    public transient Counter fileMetasInCounter;

    private boolean isAsync;
    private Connection syncHBaseConnection;
    private AsyncConnection AsyncHBaseConnection;
    private Table table;
    private AsyncTable<AdvancedScanResultConsumer> asyncTable;
    private List<Put> dataPutList;
    private List<Put> metaPutList;
    private long maxBatchCount;

    public TestKeyedCoProcessFunction(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("file_chunk_combiner", "add_file_meta");
        chunksInCounter = metricGroup.counter("chunksInCount");
        fileMetasInCounter = metricGroup.counter("fileMetasInCount");
        metricGroup.meter("numChunksInPerSecond", new MeterView(chunksInCounter));
        metricGroup.meter("numFileMetasInPerSecond", new MeterView(fileMetasInCounter));
        isAsync = configuration.getBoolean(Configs.SINK_ASYNC);
        if (isAsync) {
            AsyncHBaseConnection = HBaseConnectionUtil.getInstance(configuration).getAsyncHBaseConnection();
            asyncTable = AsyncHBaseConnection.getTable(TableName.valueOf("default:" + configuration.get(Configs.SINK_HOS_BUCKET)));
        } else {
            syncHBaseConnection = HBaseConnectionUtil.getInstance(configuration).getSyncHBaseConnection();
            table = syncHBaseConnection.getTable(TableName.valueOf("default:" + configuration.get(Configs.SINK_HOS_BUCKET)));
        }
        maxBatchCount = configuration.getInteger(Configs.SINK_BATCH_COUNT);
        dataPutList = new ArrayList<>();
        metaPutList = new ArrayList<>();
    }

    @Override
    public void processElement1(FileChunk value, Context ctx, Collector<FileChunk> out) throws IOException, InterruptedException {
        chunksInCounter.inc();
        Put dataPut = new Put(value.getUuid().getBytes());
        dataPut.addColumn("meta".getBytes(), "data".getBytes(), (value.toString()).getBytes());
        dataPutList.add(dataPut);
        if (dataPutList.size() >= maxBatchCount) {
            if (isAsync) {
                asyncTable.batch(dataPutList);
                dataPutList.clear();
            } else {
                table.batch(dataPutList, null);
                dataPutList.clear();
            }
        }
    }

    @Override
    public void processElement2(FileChunk value, Context ctx, Collector<FileChunk> out) throws IOException, InterruptedException {
        fileMetasInCounter.inc();
        Put metaPut = new Put(value.getUuid().getBytes());
        metaPut.addColumn("meta".getBytes(), "meta".getBytes(), (value.getMeta().toString()).getBytes());
        metaPutList.add(metaPut);
        if (metaPutList.size() >= maxBatchCount) {
            if (isAsync) {
                asyncTable.batch(metaPutList);
                metaPutList.clear();
            } else {
                table.batch(metaPutList, null);
                metaPutList.clear();
            }
        }
    }

    @Override
    public void close() {
        IoUtil.close(table);
        IoUtil.close(syncHBaseConnection);
        IoUtil.close(AsyncHBaseConnection);
    }
}
