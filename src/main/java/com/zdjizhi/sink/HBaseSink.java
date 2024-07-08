package com.zdjizhi.sink;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.config.Configs;
import com.zdjizhi.pojo.FileChunk;
import com.zdjizhi.utils.HBaseColumnConstants;
import com.zdjizhi.utils.HBaseConnectionUtil;
import com.zdjizhi.utils.PublicConstants;
import com.zdjizhi.utils.PublicUtil;
import org.apache.commons.jexl3.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.zdjizhi.utils.PublicConstants.*;
import static com.zdjizhi.utils.HBaseColumnConstants.*;

public class HBaseSink extends RichSinkFunction<FileChunk> {
    private static final Log LOG = LogFactory.get();

    private final Configuration configuration;
    public transient Counter chunksInCounter;
    public transient Counter chunksOutCounter;
    public transient Counter bytesInCounter;
    public transient Counter bytesOutCounter;
    public transient Counter errorChunksCounter;
    public transient Counter filesCounter;
    public transient Counter rateLimitDropChunksCounter;
    public transient Counter lessThan1KBChunksCounter;
    public transient Counter between1KBAnd5KBChunksCounter;
    public transient Counter between5KBAnd10KBChunksCounter;
    public transient Counter between10KBAnd100KBChunksCounter;
    public transient Counter between100KBAnd1MBChunksCounter;
    public transient Counter greaterThan1MBChunksCounter;
    public transient Counter lessThan10KBEmlChunksCounter;
    public transient Counter between1MBAnd10MBEmlChunksCounter;
    public transient Counter between10KBAnd100KBEmlChunksCounter;
    public transient Counter between100KBAnd1MBEmlChunksCounter;
    public transient Counter greaterThan10MBEmlChunksCounter;
    public transient Counter lessThan10KBTxtChunksCounter;
    public transient Counter between1MBAnd10MBTxtChunksCounter;
    public transient Counter between10KBAnd100KBTxtChunksCounter;
    public transient Counter between100KBAnd1MBTxtChunksCounter;
    public transient Counter greaterThan10MBTxtChunksCounter;
    public transient Counter emlChunksCounter;
    public transient Counter txtChunksCounter;
    public transient Counter htmlChunksCounter;
    public transient Counter pcapngChunksCounter;
    public transient Counter mediaChunksCounter;
    private boolean isAsync;
    private Connection syncHBaseConnection;
    private AsyncConnection asyncHBaseConnection;
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
    private long batchSize;
    private long batchInterval;
    private ScheduledExecutorService executorService;
    private long rateLimitThreshold;
    private String rateLimitExpression;
    private volatile long timestamp;
    private long count;
    private JexlExpression jexlExpression;
    private JexlContext jexlContext;

    public HBaseSink(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("file_chunk_combiner", "sink_hbase");
        errorChunksCounter = metricGroup.counter("errorChunksCount");
        chunksInCounter = metricGroup.counter("chunksInCount");
        chunksOutCounter = metricGroup.counter("chunksOutCount");
        bytesInCounter = metricGroup.counter("bytesInCount");
        bytesOutCounter = metricGroup.counter("bytesOutCount");
        filesCounter = metricGroup.counter("filesCount");
        rateLimitDropChunksCounter = metricGroup.counter("rateLimitDropChunksCount");
        metricGroup.meter("numChunksInPerSecond", new MeterView(chunksInCounter));
        metricGroup.meter("numChunksOutPerSecond", new MeterView(chunksOutCounter));
        metricGroup.meter("numBytesInPerSecond", new MeterView(bytesInCounter));
        metricGroup.meter("numBytesOutPerSecond", new MeterView(bytesOutCounter));
        metricGroup.meter("numErrorChunksPerSecond", new MeterView(errorChunksCounter));
        metricGroup.meter("numFilesOutPerSecond", new MeterView(filesCounter));
        metricGroup.meter("numChunksRateLimitDropPerSecond", new MeterView(rateLimitDropChunksCounter));
        lessThan1KBChunksCounter = metricGroup.counter("lessThan1KBChunksCount");
        between1KBAnd5KBChunksCounter = metricGroup.counter("between1KBAnd5KBChunksCount");
        between5KBAnd10KBChunksCounter = metricGroup.counter("between5KBAnd10KBChunksCount");
        between10KBAnd100KBChunksCounter = metricGroup.counter("between10KBAnd100KBChunksCount");
        between100KBAnd1MBChunksCounter = metricGroup.counter("between100KBAnd1MBChunksCount");
        greaterThan1MBChunksCounter = metricGroup.counter("greaterThan1MBChunksCount");
        metricGroup.meter("numLessThan1KBChunksOutPerSecond", new MeterView(lessThan1KBChunksCounter));
        metricGroup.meter("numBetween1KBAnd5KBChunksOutPerSecond", new MeterView(between1KBAnd5KBChunksCounter));
        metricGroup.meter("numBetween5KBAnd10KBChunksOutPerSecond", new MeterView(between5KBAnd10KBChunksCounter));
        metricGroup.meter("numBetween10KBAnd100KBChunksOutPerSecond", new MeterView(between10KBAnd100KBChunksCounter));
        metricGroup.meter("numBetween100KBAnd1MBChunksOutPerSecond", new MeterView(between100KBAnd1MBChunksCounter));
        metricGroup.meter("numGreaterThan1MBChunksOutPerSecond", new MeterView(greaterThan1MBChunksCounter));
        lessThan10KBEmlChunksCounter = metricGroup.counter("lessThan10KBEmlChunksCount");
        between10KBAnd100KBEmlChunksCounter = metricGroup.counter("between10KBAnd100KBEmlChunksCount");
        between100KBAnd1MBEmlChunksCounter = metricGroup.counter("between100KBAnd1MBEmlChunksCount");
        between1MBAnd10MBEmlChunksCounter = metricGroup.counter("between1MBAnd10MBEmlChunksCount");
        greaterThan10MBEmlChunksCounter = metricGroup.counter("greaterThan10MBEmlChunksCount");
        metricGroup.meter("numLessThan10KBEmlChunksOutPerSecond", new MeterView(lessThan10KBEmlChunksCounter));
        metricGroup.meter("numBetween10KBAnd100KBEmlChunksOutPerSecond", new MeterView(between10KBAnd100KBEmlChunksCounter));
        metricGroup.meter("numBetween100KBAnd1MBEmlChunksOutPerSecond", new MeterView(between100KBAnd1MBEmlChunksCounter));
        metricGroup.meter("numBetween1MBAnd10MBEmlChunksOutPerSecond", new MeterView(between1MBAnd10MBEmlChunksCounter));
        metricGroup.meter("numGreaterThan10MBEmlChunksOutPerSecond", new MeterView(greaterThan10MBEmlChunksCounter));
        lessThan10KBTxtChunksCounter = metricGroup.counter("lessThan10KBTxtChunksCount");
        between10KBAnd100KBTxtChunksCounter = metricGroup.counter("between10KBAnd100KBTxtChunksCount");
        between100KBAnd1MBTxtChunksCounter = metricGroup.counter("between100KBAnd1MBTxtChunksCount");
        between1MBAnd10MBTxtChunksCounter = metricGroup.counter("between1MBAnd10MBTxtChunksCount");
        greaterThan10MBTxtChunksCounter = metricGroup.counter("greaterThan10MBTxtChunksCount");
        metricGroup.meter("numLessThan10KBTxtChunksOutPerSecond", new MeterView(lessThan10KBTxtChunksCounter));
        metricGroup.meter("numBetween10KBAnd100KBTxtChunksOutPerSecond", new MeterView(between10KBAnd100KBTxtChunksCounter));
        metricGroup.meter("numBetween100KBAnd1MBTxtChunksOutPerSecond", new MeterView(between100KBAnd1MBTxtChunksCounter));
        metricGroup.meter("numBetween1MBAnd10MBTxtChunksOutPerSecond", new MeterView(between1MBAnd10MBTxtChunksCounter));
        metricGroup.meter("numGreaterThan10MBTxtChunksOutPerSecond", new MeterView(greaterThan10MBTxtChunksCounter));
        emlChunksCounter = metricGroup.counter("emlChunksCount");
        txtChunksCounter = metricGroup.counter("txtChunksCount");
        htmlChunksCounter = metricGroup.counter("htmlChunksCount");
        pcapngChunksCounter = metricGroup.counter("pcapngChunksCount");
        mediaChunksCounter = metricGroup.counter("mediaChunksCount");
        metricGroup.meter("numEmlChunksOutPerSecond", new MeterView(emlChunksCounter));
        metricGroup.meter("numTxtChunksOutPerSecond", new MeterView(txtChunksCounter));
        metricGroup.meter("numHtmlChunksOutPerSecond", new MeterView(htmlChunksCounter));
        metricGroup.meter("numPcapngChunksOutPerSecond", new MeterView(pcapngChunksCounter));
        metricGroup.meter("numMediaChunksOutPerSecond", new MeterView(mediaChunksCounter));
        isAsync = configuration.getBoolean(Configs.SINK_ASYNC);
        if (isAsync) {
            asyncHBaseConnection = HBaseConnectionUtil.getInstance(configuration).getAsyncHBaseConnection();
            asyncTable = asyncHBaseConnection.getTable(TableName.valueOf("default:" + configuration.get(Configs.SINK_HOS_BUCKET)));
            asyncIndexTimeTable = asyncHBaseConnection.getTable(TableName.valueOf("default:index_time_" + configuration.get(Configs.SINK_HOS_BUCKET)));
            asyncIndexFilenameTable = asyncHBaseConnection.getTable(TableName.valueOf("default:index_filename_" + configuration.get(Configs.SINK_HOS_BUCKET)));
        } else {
            syncHBaseConnection = HBaseConnectionUtil.getInstance(configuration).getSyncHBaseConnection();
            table = syncHBaseConnection.getTable(TableName.valueOf("default:" + configuration.get(Configs.SINK_HOS_BUCKET)));
            indexTimeTable = syncHBaseConnection.getTable(TableName.valueOf("default:index_time_" + configuration.get(Configs.SINK_HOS_BUCKET)));
            indexFilenameTable = syncHBaseConnection.getTable(TableName.valueOf("default:index_filename_" + configuration.get(Configs.SINK_HOS_BUCKET)));
        }
        timestamp = System.currentTimeMillis();
        batchSize = configuration.getLong(Configs.SINK_HBASE_BATCH_SIZE);
        batchInterval = configuration.getInteger(Configs.SINK_HBASE_BATCH_INTERVAL_MS);
        dataPutList = new ArrayList<>();
        indexTimePutList = new ArrayList<>();
        indexFilenamePutList = new ArrayList<>();
        if (batchSize > 0 && batchInterval > 0) {
            chunkSize = 0;
            executorService = Executors.newScheduledThreadPool(1);
            executorService.scheduleWithFixedDelay(() -> {
                synchronized (this) {
                    if (!dataPutList.isEmpty()) {
                        sendDataToHbase();
                    }
                }
            }, batchInterval, batchInterval, TimeUnit.MILLISECONDS);
        }
        if (rateLimitThreshold > 0) {
            rateLimitThreshold = configuration.getLong(Configs.SINK_RATE_LIMIT_THRESHOLD);
            rateLimitExpression = configuration.getString(Configs.SINK_RATE_LIMIT_EXCLUSION_EXPRESSION);
            count = 0;
            JexlEngine jexlEngine = new JexlBuilder().create();
            jexlExpression = jexlEngine.createExpression(rateLimitExpression);
            jexlContext = new MapContext();
        }
    }

    @Override
    public void invoke(FileChunk fileChunk, Context context) {
        synchronized (this) {
            long currentTimeMillis = System.currentTimeMillis();
            chunksInCounter.inc();
            bytesInCounter.inc(fileChunk.getLength());
            if (rateLimitThreshold > 0) {
                count++;
                if (currentTimeMillis - timestamp < 1000 && count > rateLimitThreshold) {
                    if (checkFileChunk(fileChunk)) {
                        sendFileChunk(fileChunk);
                    } else {
                        rateLimitDropChunksCounter.inc();
                    }
                } else if (currentTimeMillis - timestamp >= 1000) {
                    if (checkFileChunk(fileChunk)) {
                        sendFileChunk(fileChunk);
                    } else {
                        rateLimitDropChunksCounter.inc();
                    }
                    timestamp = currentTimeMillis;
                    count = 0;
                } else {
                    sendFileChunk(fileChunk);
                }
            } else {
                sendFileChunk(fileChunk);
            }
        }
    }

    @Override
    public void close() {
        IoUtil.close(table);
        IoUtil.close(indexTimeTable);
        IoUtil.close(indexFilenameTable);
        IoUtil.close(syncHBaseConnection);
        IoUtil.close(asyncHBaseConnection);
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    private void sendFileChunk(FileChunk fileChunk) {
        if (COMBINE_MODE_SEEK.equals(fileChunk.getCombineMode())) {
            byte[] data = "".getBytes();
            if (fileChunk.getChunk() != null) {
                data = fileChunk.getChunk();
            }
            int chunkLength = data.length;
            long timestamp = System.currentTimeMillis();
            Map<String, String> partMessageMap = new HashMap<>();
            partMessageMap.put(APPEND_FILE_PART_MESSAGE_CHUNK_COUNT, fileChunk.getChunkCount() + "");
            partMessageMap.put(APPEND_FILE_PART_MESSAGE_LAST_PART_FLAG, fileChunk.getLastChunkFlag() + "");
            partMessageMap.put(APPEND_FILE_PART_MESSAGE_SIZE, chunkLength + "");
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
            } else {
                Put metaPut = new Put(Bytes.toBytes(PublicUtil.getRowKey(fileChunk.getUuid())));
                metaPut.addColumn(HBaseColumnConstants.BYTE_FAMILY_META, HBaseColumnConstants.BYTE_COLUMN_LAST_MODIFIED, Bytes.toBytes(timestamp));
                dataPutList.add(metaPut);
            }
            chunkSize += chunkLength;
            chunksOutCounter.inc();
            bytesOutCounter.inc(chunkLength);
            calculateFileChunkMetrics(fileChunk);
            if (batchSize > 0 && batchInterval > 0) {
                if (chunkSize >= batchSize) {
                    sendDataToHbase();
                }
            } else {
                sendDataToHbase();
            }
        }
    }

    private void sendDataToHbase() {
        if (isAsync) {
            List<CompletableFuture<Object>> futures = asyncTable.batch(dataPutList);
            CompletableFuture.supplyAsync(() -> {
                for (CompletableFuture<Object> completableFuture : futures) {
                    completableFuture.whenCompleteAsync((result, error) -> {
                        if (error != null) {
                            LOG.error("Put chunk to hbase error. ", error.getMessage());
                            errorChunksCounter.inc();
                        }
                    });
                }
                return null;
            });
            dataPutList.clear();
            if (indexTimePutList.size() > 0) {
                asyncIndexTimeTable.batch(indexTimePutList);
                indexTimePutList.clear();
            }
            if (indexFilenamePutList.size() > 0) {
                asyncIndexFilenameTable.batch(indexFilenamePutList);
                indexFilenamePutList.clear();
            }
        } else {
            try {
                table.batch(dataPutList, null);
                if (indexTimePutList.size() > 0) {
                    indexTimeTable.batch(indexTimePutList, null);
                }
                if (indexFilenamePutList.size() > 0) {
                    indexFilenameTable.batch(indexFilenamePutList, null);
                }
            } catch (IOException | InterruptedException e) {
                LOG.error("Put chunk to hbase error. ", e.getMessage());
                errorChunksCounter.inc(dataPutList.size());
                Thread.currentThread().interrupt();
            } finally {
                dataPutList.clear();
                indexTimePutList.clear();
                indexFilenamePutList.clear();
            }
        }
        chunkSize = 0;
    }

    private boolean checkFileChunk(FileChunk fileChunk) {
        if (StrUtil.isNotEmpty(rateLimitExpression)) {
            jexlContext.set(fileChunk.getClass().getSimpleName(), fileChunk);
            return Boolean.parseBoolean(jexlExpression.evaluate(jexlContext).toString());
        }
        return false;
    }

    private void calculateFileChunkMetrics(FileChunk fileChunk) {
        long length = fileChunk.getLength();
        String fileType = fileChunk.getFileType();
        if (length <= 1024) {
            lessThan1KBChunksCounter.inc();
        } else if (length <= 5 * 1024) {
            between1KBAnd5KBChunksCounter.inc();
        } else if (length <= 10 * 1024) {
            between5KBAnd10KBChunksCounter.inc();
        } else if (length <= 100 * 1024) {
            between10KBAnd100KBChunksCounter.inc();
        } else if (length <= 1024 * 1024) {
            between100KBAnd1MBChunksCounter.inc();
        } else {
            greaterThan1MBChunksCounter.inc();
        }
        switch (fileType) {
            case "eml":
                emlChunksCounter.inc();
                if (length <= 10 * 1024) {
                    lessThan10KBEmlChunksCounter.inc();
                } else if (length <= 100 * 1024) {
                    between10KBAnd100KBEmlChunksCounter.inc();
                } else if (length <= 1024 * 1024) {
                    between100KBAnd1MBEmlChunksCounter.inc();
                } else if (length <= 10 * 1024 * 1024) {
                    between1MBAnd10MBEmlChunksCounter.inc();
                } else {
                    greaterThan10MBEmlChunksCounter.inc();
                }
                break;
            case "html":
                htmlChunksCounter.inc();
                break;
            case "txt":
                txtChunksCounter.inc();
                if (length <= 10 * 1024) {
                    lessThan10KBTxtChunksCounter.inc();
                } else if (length <= 100 * 1024) {
                    between10KBAnd100KBTxtChunksCounter.inc();
                } else if (length <= 1024 * 1024) {
                    between100KBAnd1MBTxtChunksCounter.inc();
                } else if (length <= 10 * 1024 * 1024) {
                    between1MBAnd10MBTxtChunksCounter.inc();
                } else {
                    greaterThan10MBTxtChunksCounter.inc();
                }
                break;
            case "pcapng":
                pcapngChunksCounter.inc();
                break;
            default:
                mediaChunksCounter.inc();
        }
        if (fileChunk.getLastChunkFlag() == 1) {
            filesCounter.inc();
        }
    }
}
