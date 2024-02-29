package com.zdjizhi.sink;

import cn.hutool.core.io.IoUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.config.Configs;
import com.zdjizhi.pojo.FileChunk;
import com.zdjizhi.utils.HBaseColumnConstants;
import com.zdjizhi.utils.HBaseConnectionUtil;
import com.zdjizhi.utils.PublicConstants;
import com.zdjizhi.utils.PublicUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static com.zdjizhi.utils.PublicConstants.*;
import static com.zdjizhi.utils.HBaseColumnConstants.*;

public class HBaseSink extends RichSinkFunction<FileChunk> {
    private static final Log LOG = LogFactory.get();

    private final Configuration configuration;
    private transient Counter sendHBaseCounter;
    private transient Counter sendHBaseErrorCounter;
    private transient Counter sendHBaseFileCounter;
    private transient Counter sendHBaseChunkCounter;
    private boolean isAsync;
    private Connection syncHBaseConnection;
    private AsyncConnection AsyncHBaseConnection;
    private Table table;
    private Table indexTimeTable;
    private Table indexFilenameTable;
    private AsyncTable<AdvancedScanResultConsumer> asyncTable;
    private AsyncTable<AdvancedScanResultConsumer> asyncIndexTimeTable;
    private AsyncTable<AdvancedScanResultConsumer> asyncIndexFilenameTable;
    private List<Put> dataPutList;
    private List<Put> indexTimePutList;
    private List<Put> indexFilenamePutList;
    private long chunkSize;
    private int chunkCount;
    private long maxBatchSize;
    private long maxBatchCount;

    public HBaseSink(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
        sendHBaseCounter = metricGroup.counter("sendHBaseCount");
        sendHBaseErrorCounter = metricGroup.counter("sendHBaseErrorCount");
        sendHBaseFileCounter = metricGroup.counter("sendHBaseFileCount");
        sendHBaseChunkCounter = metricGroup.counter("sendHBaseChunkCount");
        isAsync = configuration.getBoolean(Configs.SINK_ASYNC);
        if (isAsync) {
            AsyncHBaseConnection = HBaseConnectionUtil.getInstance(configuration).getAsyncHBaseConnection();
            asyncTable = AsyncHBaseConnection.getTable(TableName.valueOf("default:" + configuration.get(Configs.SINK_HOS_BUCKET)));
            asyncIndexTimeTable = AsyncHBaseConnection.getTable(TableName.valueOf("default:index_time_" + configuration.get(Configs.SINK_HOS_BUCKET)));
            asyncIndexFilenameTable = AsyncHBaseConnection.getTable(TableName.valueOf("default:index_filename_" + configuration.get(Configs.SINK_HOS_BUCKET)));
        } else {
            syncHBaseConnection = HBaseConnectionUtil.getInstance(configuration).getSyncHBaseConnection();
            table = syncHBaseConnection.getTable(TableName.valueOf("default:" + configuration.get(Configs.SINK_HOS_BUCKET)));
            indexTimeTable = syncHBaseConnection.getTable(TableName.valueOf("default:index_time_" + configuration.get(Configs.SINK_HOS_BUCKET)));
            indexFilenameTable = syncHBaseConnection.getTable(TableName.valueOf("default:index_filename_" + configuration.get(Configs.SINK_HOS_BUCKET)));
        }
        maxBatchSize = configuration.getLong(Configs.SINK_BATCH_SIZE);
        maxBatchCount = configuration.getInteger(Configs.SINK_BATCH_COUNT);
        dataPutList = new ArrayList<>();
        indexTimePutList = new ArrayList<>();
        indexFilenamePutList = new ArrayList<>();
        chunkSize = 0;
        chunkCount = 0;
    }

    @Override
    public void invoke(FileChunk fileChunk, Context context) {
        if (COMBINE_MODE_SEEK.equals(fileChunk.getCombineMode()) && configuration.get(Configs.SINK_BATCH)) {
            sendHBaseChunkCounter.inc();
            byte[] data = "".getBytes();
            if (fileChunk.getChunk() != null) {
                data = fileChunk.getChunk();
            }
            long timestamp = System.currentTimeMillis();
            Map<String, String> partMessageMap = new HashMap<>();
            partMessageMap.put(APPEND_FILE_PART_MESSAGE_CHUNK_COUNT, fileChunk.getChunkCount() + "");
            partMessageMap.put(APPEND_FILE_PART_MESSAGE_LAST_PART_FLAG, fileChunk.getLastChunkFlag() + "");
            partMessageMap.put(APPEND_FILE_PART_MESSAGE_SIZE, data.length + "");
            Put dataPut = new Put(Bytes.toBytes(PublicUtil.getRowKey(fileChunk.getUuid()) + PublicConstants.FILE_DATA_ROW_SUFFIX));
            dataPut.addColumn(BYTE_FAMILY_DATA, Bytes.toBytes(String.valueOf(fileChunk.getOffset())), data);
            dataPut.addColumn(BYTE_FAMILY_META, Bytes.toBytes(String.valueOf(fileChunk.getOffset())), Bytes.toBytes(partMessageMap.toString()));
            dataPutList.add(dataPut);
            if (fileChunk.getOffset() == 0) {
                Put metaPut = new Put(PublicUtil.getRowKey(fileChunk.getUuid()).getBytes());
                String filename = fileChunk.getUuid() + "." + fileChunk.getFileType();
                String randomIndexHead = PublicUtil.getIndexDataHead(filename);
                String indexTimeKey = randomIndexHead + "|" + timestamp + "|" + PublicUtil.getRowKey(fileChunk.getUuid());
                String indexFilenameKey = randomIndexHead + "|" + filename;
                metaPut.addColumn(BYTE_FAMILY_META, Bytes.toBytes(COLUMN_USER_DEFINED_META_PREFIX + FILE_META_FILE_TYPE), Bytes.toBytes(fileChunk.getFileType()));
                metaPut.addColumn(BYTE_FAMILY_META, BYTE_COLUMN_CONTENT_TYPE, Bytes.toBytes("application/octet-stream"));
                metaPut.addColumn(BYTE_FAMILY_META, BYTE_COLUMN_FILENAME, Bytes.toBytes(filename));
                metaPut.addColumn(BYTE_FAMILY_META, BYTE_COLUMN_UPLOAD_METHOD, Bytes.toBytes(String.valueOf(5)));
                metaPut.addColumn(BYTE_FAMILY_META, BYTE_COLUMN_INDEX_FILENAME_KEY, Bytes.toBytes(indexTimeKey));
                metaPut.addColumn(BYTE_FAMILY_META, BYTE_COLUMN_INDEX_TIME_KEY, Bytes.toBytes(indexFilenameKey));
                metaPut.addColumn(BYTE_FAMILY_META, BYTE_COLUMN_LAST_MODIFIED, Bytes.toBytes(timestamp));
                metaPut.addColumn(BYTE_FAMILY_META, BYTE_COLUMN_APPEND_MODE, Bytes.toBytes(APPEND_MODE_OFFSET));
                metaPut.addColumn(BYTE_FAMILY_META, BYTE_COLUMN_COMBINE_MODE, Bytes.toBytes(fileChunk.getCombineMode()));
                dataPutList.add(metaPut);
                Put indexTimePut = new Put(Bytes.toBytes(indexTimeKey));
                indexTimePut.addColumn(HBaseColumnConstants.BYTE_FAMILY_META, HBaseColumnConstants.BYTE_COLUMN_FILENAME, Bytes.toBytes(fileChunk.getUuid()));
                indexTimePutList.add(indexTimePut);
                Put indexFilenamePut = new Put(Bytes.toBytes(indexFilenameKey));
                indexFilenamePut.addColumn(HBaseColumnConstants.BYTE_FAMILY_META, HBaseColumnConstants.BYTE_COLUMN_FILENAME, Bytes.toBytes(fileChunk.getUuid()));
                indexFilenamePutList.add(indexFilenamePut);
                sendHBaseFileCounter.inc();
            } else {
                Put metaPut = new Put(Bytes.toBytes(PublicUtil.getRowKey(fileChunk.getUuid())));
                metaPut.addColumn(HBaseColumnConstants.BYTE_FAMILY_META, HBaseColumnConstants.BYTE_COLUMN_LAST_MODIFIED, Bytes.toBytes(timestamp));
                dataPutList.add(metaPut);
            }
            chunkCount++;
            chunkSize += data.length;
            if (chunkSize >= maxBatchSize || chunkCount >= maxBatchCount) {
                if (isAsync) {
                    if (dataPutList.size() > 0) {
                        List<CompletableFuture<Object>> futures = asyncTable.batch(dataPutList);
                        sendHBaseCounter.inc();
                        CompletableFuture.supplyAsync(() -> {
                            for (CompletableFuture<Object> completableFuture : futures) {
                                completableFuture.whenCompleteAsync((result, error) -> {
                                    if (error != null) {
                                        LOG.error("put chunk to hbase error. ", error.getMessage());
                                        sendHBaseErrorCounter.inc();
                                    }
                                });
                            }
                            return null;
                        });
                        dataPutList.clear();
                    }
                    if (indexTimePutList.size() > 0) {
                        asyncIndexTimeTable.batch(indexTimePutList);
                        sendHBaseCounter.inc();
                        indexTimePutList.clear();
                    }
                    if (indexFilenamePutList.size() > 0) {
                        asyncIndexFilenameTable.batch(indexFilenamePutList);
                        sendHBaseCounter.inc();
                        indexFilenamePutList.clear();
                    }
                } else {
                    if (dataPutList.size() > 0) {
                        try {
                            sendHBaseCounter.inc();
                            table.batch(dataPutList, null);
                        } catch (IOException | InterruptedException e) {
                            LOG.error("put chunk to hbase data table error. ", e.getMessage());
                            sendHBaseErrorCounter.inc();
                        }finally {
                            dataPutList.clear();
                        }
                    }
                    if (indexTimePutList.size() > 0) {
                        try {
                            sendHBaseCounter.inc();
                            indexTimeTable.batch(indexTimePutList, null);
                        } catch (IOException | InterruptedException e) {
                            LOG.error("put chunk to hbase index time table error. ", e.getMessage());
                            sendHBaseErrorCounter.inc();
                        }finally {
                            indexTimePutList.clear();
                        }
                    }
                    if (indexFilenamePutList.size() > 0) {
                        try {
                            sendHBaseCounter.inc();
                            indexFilenameTable.batch(indexFilenamePutList, null);
                        } catch (IOException | InterruptedException e) {
                            LOG.error("put chunk to hbase index filename table error. ", e.getMessage());
                            sendHBaseErrorCounter.inc();
                        }finally {
                            indexFilenamePutList.clear();
                        }
                    }
                }
                chunkSize = 0;
                chunkCount = 0;
            }
        }
    }

    @Override
    public void close() {
        IoUtil.close(table);
        IoUtil.close(indexTimeTable);
        IoUtil.close(indexFilenameTable);
        IoUtil.close(syncHBaseConnection);
        IoUtil.close(AsyncHBaseConnection);
    }

}
