package com.zdjizhi.sink;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.*;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.config.Configs;
import com.zdjizhi.pojo.FileChunk;
import com.zdjizhi.utils.HttpClientUtil;
import com.zdjizhi.utils.PublicUtil;
import org.apache.commons.jexl3.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.ConnectException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.zdjizhi.utils.HttpHeaderConstants.*;
import static com.zdjizhi.utils.PublicConstants.*;

public class HosSink extends RichSinkFunction<FileChunk> {
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
    private CloseableHttpClient syncHttpClient;
    private CloseableHttpAsyncClient asyncHttpClient;
    private int loadBalanceMode;
    private List<String> endpointList;
    private volatile String endpoint;
    private String token;
    private volatile String bathPutUrl;
    private HashMap<String, String> hosMessage;
    private String objectsMeta;
    private String objectsOffset;
    private List<byte[]> byteList;
    private long batchSize;
    private long batchInterval;
    private long chunkSize;
    private ScheduledExecutorService executorService;
    private long rateLimitThreshold;
    private String rateLimitExpression;
    private volatile long timestamp;
    private long count;
    private JexlExpression jexlExpression;
    private JexlContext jexlContext;

    public HosSink(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("file_chunk_combiner", "sink_hos");
        errorChunksCounter = metricGroup.counter("errorChunksCount");
        chunksInCounter = metricGroup.counter("chunksInCount");
        bytesInCounter = metricGroup.counter("bytesInCount");
        chunksOutCounter = metricGroup.counter("chunksOutCount");
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
        endpointList = Arrays.asList(configuration.get(Configs.SINK_HOS_ENDPOINT).split(","));
        if (endpointList.size() == 1) {
            loadBalanceMode = 0;
            endpoint = endpointList.get(0);
        } else {
            loadBalanceMode = 1;
            endpoint = endpointList.get(RandomUtil.randomInt(endpointList.size()));
        }
        token = configuration.get(Configs.SINK_HOS_TOKEN);
        isAsync = configuration.getBoolean(Configs.SINK_ASYNC);
        if (isAsync) {
            asyncHttpClient = HttpClientUtil.getInstance(configuration).getAsyncHttpClient();
            asyncHttpClient.start();
        } else {
            syncHttpClient = HttpClientUtil.getInstance(configuration).getSyncHttpClient();
        }
        timestamp = System.currentTimeMillis();
        batchSize = configuration.getLong(Configs.SINK_HOS_BATCH_SIZE);
        batchInterval = configuration.getInteger(Configs.SINK_HOS_BATCH_INTERVAL_MS);
        if (batchSize > 0 && batchInterval > 0) {
            bathPutUrl = URLUtil.normalize(endpoint + "/hos/" + configuration.get(Configs.SINK_HOS_BUCKET) + "/" + PublicUtil.getUUID()) + "?multiFile";
            hosMessage = new HashMap<>();
            byteList = new ArrayList<>();
            objectsMeta = "";
            objectsOffset = "";
            chunkSize = 0;
            executorService = Executors.newScheduledThreadPool(1);
            executorService.scheduleWithFixedDelay(() -> {
                synchronized (this) {
                    if (!byteList.isEmpty()) {
                        sendBatchData();
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
        IoUtil.close(syncHttpClient);
        IoUtil.close(asyncHttpClient);
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    private void sendFileChunk(FileChunk fileChunk) {
        try {
            byte[] data = "".getBytes();
            if (fileChunk.getChunk() != null) {
                data = fileChunk.getChunk();
            }
            long chunkLength = data.length;
            if (batchSize > 0 && batchInterval > 0) {
                hosMessage.put(HOS_META_FILE_TYPE, fileChunk.getFileType());
                hosMessage.put(HOS_META_FILENAME, fileChunk.getUuid());
                if (COMBINE_MODE_SEEK.equals(fileChunk.getCombineMode())) {
                    hosMessage.put(HOS_OFFSET, fileChunk.getOffset() + "");
                    hosMessage.put(HOS_PART_LAST_FLAG, fileChunk.getLastChunkFlag() + "");
                } else {
                    hosMessage.put(HOS_PART_NUMBER, fileChunk.getTimestamp() + "");
                    hosMessage.put(HOS_PART_CHUNK_NUMBERS, fileChunk.getChunkNumbers());
                }
                hosMessage.put(HOS_PART_CHUNK_COUNT, fileChunk.getChunkCount() + "");
                Map<String, Object> metaMap = fileChunk.getMeta();
                if (metaMap != null && !metaMap.isEmpty()) {
                    for (String meta : metaMap.keySet()) {
                        hosMessage.put(HOS_META_PREFIX + StrUtil.toSymbolCase(meta, CharUtil.DASHED), metaMap.get(meta) + "");
                    }
                }
                objectsMeta += hosMessage.toString() + ";";
                hosMessage.clear();
                objectsOffset += chunkLength + ";";
                byteList.add(data);
                chunkSize += chunkLength;
                chunksOutCounter.inc();
                bytesOutCounter.inc(chunkLength);
                calculateFileChunkMetrics(fileChunk);
                if (chunkSize >= batchSize) {
                    sendBatchData();
                }
            } else {
                String url = URLUtil.normalize(endpoint + "/hos/" + configuration.get(Configs.SINK_HOS_BUCKET) + "/" + fileChunk.getUuid());
                HttpPut httpPut = new HttpPut(url);
                httpPut.setHeader(TOKEN, configuration.get(Configs.SINK_HOS_TOKEN));
                httpPut.setHeader(HOS_UPLOAD_TYPE, UPLOAD_TYPE_APPENDV2);
                httpPut.setHeader(HOS_COMBINE_MODE, fileChunk.getCombineMode());
                String filename = fileChunk.getFileName();
                if (StrUtil.isNotEmpty(filename) && filename.contains(".")) {
                    httpPut.setHeader(HOS_META_FILENAME, filename);
                } else if (StrUtil.isNotEmpty(filename) && !filename.contains(".")) {
                    filename = filename + "." + fileChunk.getFileType();
                    httpPut.setHeader(HOS_META_FILENAME, filename);
                } else if (StrUtil.isEmpty(filename) && StrUtil.isNotEmpty(fileChunk.getFileType())) {
                    httpPut.setHeader(HOS_META_FILE_TYPE, fileChunk.getFileType());
                }
                if (COMBINE_MODE_SEEK.equals(fileChunk.getCombineMode())) {
                    httpPut.setHeader(HOS_OFFSET, fileChunk.getOffset() + "");
                    httpPut.setHeader(HOS_PART_LAST_FLAG, fileChunk.getLastChunkFlag() + "");
                } else {
                    httpPut.setHeader(HOS_PART_NUMBER, fileChunk.getTimestamp() + "");
                    httpPut.setHeader(HOS_PART_CHUNK_NUMBERS, fileChunk.getChunkNumbers());
                }
                httpPut.setHeader(HOS_PART_CHUNK_COUNT, fileChunk.getChunkCount() + "");
                Map<String, Object> metaMap = fileChunk.getMeta();
                if (metaMap != null && !metaMap.isEmpty()) {
                    for (String meta : metaMap.keySet()) {
                        httpPut.setHeader(HOS_META_PREFIX + StrUtil.toSymbolCase(meta, CharUtil.DASHED), metaMap.get(meta) + "");
                    }
                }
                httpPut.setEntity(new ByteArrayEntity(data));
                executeRequest(httpPut);
                chunksOutCounter.inc();
                bytesOutCounter.inc(chunkLength);
                calculateFileChunkMetrics(fileChunk);
            }
        } catch (Exception e) {
            LOG.error("put part to hos error.", e);
            errorChunksCounter.inc();
        }
    }

    private void sendBatchData() {
        HttpPut httpPut = new HttpPut(bathPutUrl);
        httpPut.setHeader(TOKEN, token);
        httpPut.setHeader(HOS_UPLOAD_TYPE, UPLOAD_TYPE_APPENDV2);
        httpPut.setHeader(HOS_COMBINE_MODE, COMBINE_MODE_SEEK);
        httpPut.setHeader(HOS_OBJECTS_META, objectsMeta);
        httpPut.setHeader(HOS_OBJECTS_OFFSET, objectsOffset);
        byte[][] bytes = new byte[byteList.size()][];
        byteList.toArray(bytes);
        byte[] newData = ArrayUtil.addAll(bytes);
        httpPut.setEntity(new ByteArrayEntity(newData));
        executeRequest(httpPut);
        objectsMeta = "";
        objectsOffset = "";
        byteList.clear();
        chunkSize = 0;
    }

    private void executeRequest(HttpPut httpPut) {
        if (isAsync) {
            asyncHttpClient.execute(httpPut, new FutureCallback<HttpResponse>() {
                @Override
                public void completed(HttpResponse httpResponse) {
                    try {
                        if (httpResponse.getStatusLine().getStatusCode() != 200) {
                            String responseEntity = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
                            LOG.error("put part to hos error. code: " + httpResponse.getStatusLine().getStatusCode() + ". message: " + responseEntity);
                            errorChunksCounter.inc();
                        }
                    } catch (IOException e) {
                        LOG.error("put part to hos error.", e);
                        errorChunksCounter.inc();
                    }
                }

                @Override
                public void failed(Exception ex) {
                    LOG.error("put part to hos error.", ex);
                    errorChunksCounter.inc();
                    if (loadBalanceMode == 1 && ex instanceof ConnectException) {
                        endpoint = endpointList.get(RandomUtil.randomInt(endpointList.size()));
                        bathPutUrl = URLUtil.normalize(endpoint + "/hos/" + configuration.get(Configs.SINK_HOS_BUCKET) + "/" + PublicUtil.getUUID()) + "?multiFile";
                    }
                }

                @Override
                public void cancelled() {

                }
            });
        } else {
            CloseableHttpResponse response = null;
            try {
                response = syncHttpClient.execute(httpPut);
                if (response.getStatusLine().getStatusCode() != 200) {
                    String responseEntity = EntityUtils.toString(response.getEntity(), "UTF-8");
                    LOG.error("put part to hos error. code: " + response.getStatusLine().getStatusCode() + ". message: " + responseEntity);
                    errorChunksCounter.inc();
                }
            } catch (IOException e) {
                LOG.error("put part to hos error.", e);
                errorChunksCounter.inc();
                if (loadBalanceMode == 1 && (e instanceof HttpHostConnectException || e instanceof ConnectTimeoutException)) {
                    endpoint = endpointList.get(RandomUtil.randomInt(endpointList.size()));
                }
            } finally {
                IoUtil.close(response);
            }
        }
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
