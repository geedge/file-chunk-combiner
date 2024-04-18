package com.zdjizhi.sink;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.URLUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.github.benmanes.caffeine.cache.Cache;
import com.zdjizhi.config.Configs;
import com.zdjizhi.pojo.FileChunk;
import com.zdjizhi.utils.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class OssSinkByCaffeineCache extends RichSinkFunction<FileChunk> {
    private static final Log LOG = LogFactory.get();

    private final Configuration configuration;
    public transient Counter chunksInCounter;
    public transient Counter chunksOutCounter;
    public transient Counter bytesInCounter;
    public transient Counter bytesOutCounter;
    public transient Counter errorChunksCounter;
    public transient Counter fileMetasCounter;
    public transient Counter requestFileMetasCounter;
    public transient Counter responseFileMetasCounter;
    public transient Counter requestFilesCounter;
    public transient Counter responseFilesCounter;
    public transient Counter emlChunksCounter;
    public transient Counter txtChunksCounter;
    public transient Counter completeFilesCounter;
    public transient Counter completeEmlFilesCounter;
    public transient Counter completeTxtFilesCounter;
    public transient Counter completeRequestFilesCounter;
    public transient Counter completeResponseFilesCounter;
    public transient Counter nullChunksCounter;
    public transient Counter nullTxtChunksCounter;
    public transient Counter nullEmlChunksCounter;
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
    private boolean isAsync;
    private CloseableHttpClient syncHttpClient;
    private CloseableHttpAsyncClient asyncHttpClient;
    private List<String> endpointList;
    private CaffeineCacheUtil caffeineCacheUtil;
    private Cache<String, FileChunk> cache;

    public OssSinkByCaffeineCache(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("file_chunk_combiner", "sink_oss");
        endpointList = Arrays.asList(configuration.get(Configs.SINK_OSS_ENDPOINT).split(","));
        isAsync = configuration.getBoolean(Configs.SINK_OSS_ASYNC);
        if (isAsync) {
            asyncHttpClient = HttpClientUtil.getInstance(configuration).getAsyncHttpClient();
            asyncHttpClient.start();
        } else {
            syncHttpClient = HttpClientUtil.getInstance(configuration).getSyncHttpClient();
        }
        caffeineCacheUtil = CaffeineCacheUtil.getInstance(configuration);
        cache = caffeineCacheUtil.getCaffeineCache();
        metricGroup.gauge("cacheLength", (Gauge<Long>) () -> cache.estimatedSize());
        lessThan1KBChunksCounter = metricGroup.counter("lessThan1KBChunksCount");
        between1KBAnd5KBChunksCounter = metricGroup.counter("between1KBAnd5KBChunksCount");
        between5KBAnd10KBChunksCounter = metricGroup.counter("between5KBAnd10KBChunksCount");
        between10KBAnd100KBChunksCounter = metricGroup.counter("between10KBAnd100KBChunksCount");
        between100KBAnd1MBChunksCounter = metricGroup.counter("between100KBAnd1MBChunksCount");
        greaterThan1MBChunksCounter = metricGroup.counter("greaterThan1MBChunksCount");
        metricGroup.meter("numLessThan1KBFilesOutPerSecond", new MeterView(lessThan1KBChunksCounter));
        metricGroup.meter("numBetween1KBAnd5KBFilesOutPerSecond", new MeterView(between1KBAnd5KBChunksCounter));
        metricGroup.meter("numBetween5KBAnd10KBFilesOutPerSecond", new MeterView(between5KBAnd10KBChunksCounter));
        metricGroup.meter("numBetween10KBAnd100KBFilesOutPerSecond", new MeterView(between10KBAnd100KBChunksCounter));
        metricGroup.meter("numBetween100KBAnd1MBFilesOutPerSecond", new MeterView(between100KBAnd1MBChunksCounter));
        metricGroup.meter("numGreaterThan1MBFilesOutPerSecond", new MeterView(greaterThan1MBChunksCounter));
        lessThan10KBEmlChunksCounter = metricGroup.counter("lessThan10KBEmlChunksCount");
        between10KBAnd100KBEmlChunksCounter = metricGroup.counter("between10KBAnd100KBEmlChunksCount");
        between100KBAnd1MBEmlChunksCounter = metricGroup.counter("between100KBAnd1MBEmlChunksCount");
        between1MBAnd10MBEmlChunksCounter = metricGroup.counter("between1MBAnd10MBEmlChunksCount");
        greaterThan10MBEmlChunksCounter = metricGroup.counter("greaterThan10MBEmlChunksCount");
        metricGroup.meter("numLessThan10KBEmlFilesOutPerSecond", new MeterView(lessThan10KBEmlChunksCounter));
        metricGroup.meter("numBetween10KBAnd100KBEmlFilesOutPerSecond", new MeterView(between10KBAnd100KBEmlChunksCounter));
        metricGroup.meter("numBetween100KBAnd1MBEmlFilesOutPerSecond", new MeterView(between100KBAnd1MBEmlChunksCounter));
        metricGroup.meter("numBetween1MBAnd10MBEmlFilesOutPerSecond", new MeterView(between1MBAnd10MBEmlChunksCounter));
        metricGroup.meter("numGreaterThan10MBEmlFilesOutPerSecond", new MeterView(greaterThan10MBEmlChunksCounter));
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
        metricGroup.meter("numEmlChunksOutPerSecond", new MeterView(emlChunksCounter));
        metricGroup.meter("numTxtChunksOutPerSecond", new MeterView(txtChunksCounter));
        fileMetasCounter = metricGroup.counter("fileMetasCount");
        metricGroup.meter("numFileMetasInPerSecond", new MeterView(fileMetasCounter));
        requestFileMetasCounter = metricGroup.counter("requestFileMetasCount");
        responseFileMetasCounter = metricGroup.counter("responseFileMetasCount");
        requestFilesCounter = metricGroup.counter("requestFilesCount");
        responseFilesCounter = metricGroup.counter("responseFilesCount");
        metricGroup.meter("numRequestFileMetasInPerSecond", new MeterView(requestFileMetasCounter));
        metricGroup.meter("numResponseFileMetasInPerSecond", new MeterView(responseFileMetasCounter));
        metricGroup.meter("numRequestFilesOutPerSecond", new MeterView(requestFilesCounter));
        metricGroup.meter("numResponseFilesOutPerSecond", new MeterView(responseFilesCounter));
        errorChunksCounter = metricGroup.counter("errorChunksCount");
        chunksInCounter = metricGroup.counter("chunksInCount");
        chunksOutCounter = metricGroup.counter("chunksOutCount");
        bytesInCounter = metricGroup.counter("bytesInCount");
        bytesOutCounter = metricGroup.counter("bytesOutCount");
        metricGroup.meter("numChunksInPerSecond", new MeterView(chunksInCounter));
        metricGroup.meter("numChunksOutPerSecond", new MeterView(chunksOutCounter));
        metricGroup.meter("numBytesInPerSecond", new MeterView(bytesInCounter));
        metricGroup.meter("numBytesOutPerSecond", new MeterView(bytesOutCounter));
        metricGroup.meter("numErrorChunksPerSecond", new MeterView(errorChunksCounter));
        completeFilesCounter = metricGroup.counter("completeFilesCount");
        completeEmlFilesCounter = metricGroup.counter("completeEmlFilesCount");
        completeTxtFilesCounter = metricGroup.counter("completeTxtFilesCount");
        completeRequestFilesCounter = metricGroup.counter("completeRequestFilesCount");
        completeResponseFilesCounter = metricGroup.counter("completeResponseFilesCount");
        metricGroup.meter("numCompleteFilesOutPerSecond", new MeterView(completeFilesCounter));
        metricGroup.meter("numCompleteEmlFilesOutPerSecond", new MeterView(completeEmlFilesCounter));
        metricGroup.meter("numCompleteTxtFilesOutPerSecond", new MeterView(completeTxtFilesCounter));
        metricGroup.meter("numCompleteRequestFilesOutPerSecond", new MeterView(completeRequestFilesCounter));
        metricGroup.meter("numCompleteResponseFilesOutPerSecond", new MeterView(completeResponseFilesCounter));
        nullChunksCounter = metricGroup.counter("nullChunksCount");
        nullEmlChunksCounter = metricGroup.counter("nullTxtChunksCount");
        nullTxtChunksCounter = metricGroup.counter("nullEmlChunksCount");
        metricGroup.meter("numNullFilesOutPerSecond", new MeterView(nullChunksCounter));
        metricGroup.meter("numNullEmlFilesOutPerSecond", new MeterView(nullEmlChunksCounter));
        metricGroup.meter("numNullTxtFilesOutPerSecond", new MeterView(nullTxtChunksCounter));
    }

    @Override
    public void invoke(FileChunk fileChunk, Context context) {
        String uuid = fileChunk.getUuid();
        if (fileChunk.getMeta() != null) { //日志
            fileMetasCounter.inc();
            Map<String, Object> meta = fileChunk.getMeta();
            String fileId = meta.get("fileId").toString();
            if (fileId.contains("_1")) {
                requestFileMetasCounter.inc();
            } else if (fileId.contains("_2")) {
                responseFileMetasCounter.inc();
            }
            FileChunk data = cache.getIfPresent(uuid + "_data");
            if (data != null) {
                sendFile(data, meta);
                cache.invalidate(uuid + "_data");
            } else {
                cache.put(fileChunk.getUuid() + "_meta", fileChunk);
            }
        } else { //文件
            chunksInCounter.inc();
            bytesInCounter.inc(fileChunk.getLength());
            FileChunk meta = cache.getIfPresent(uuid + "_meta");
            if (meta != null) {
                sendFile(fileChunk, meta.getMeta());
                cache.invalidate(uuid + "_meta");
            } else {
                cache.put(fileChunk.getUuid() + "_data", fileChunk);
            }
        }
    }

    @Override
    public void close() {
        IoUtil.close(syncHttpClient);
        IoUtil.close(asyncHttpClient);
        caffeineCacheUtil.close();
    }

    private void sendFile(FileChunk fileChunk, Map<String, Object> metaMap) {
        String url = "";
        try {
            byte[] data;
            String fileType = fileChunk.getFileType();
            if (fileChunk.getChunk() != null) {
                data = fileChunk.getChunk();
            } else {
                data = "".getBytes();
            }
            String fileId = metaMap != null && metaMap.containsKey("fileId") ? metaMap.get("fileId").toString() : "";
            String policyId = metaMap != null && metaMap.containsKey("policyId") ? metaMap.get("policyId").toString() : "0";
            String serverIP = metaMap != null && metaMap.containsKey("serverIP") ? metaMap.get("serverIP").toString() : "";
            String serverPort = metaMap != null && metaMap.containsKey("serverPort") ? metaMap.get("serverPort").toString() : "";
            String clientIP = metaMap != null && metaMap.containsKey("clientIP") ? metaMap.get("clientIP").toString() : "";
            String clientPort = metaMap != null && metaMap.containsKey("clientPort") ? metaMap.get("clientPort").toString() : "";
            String domain = metaMap != null && metaMap.containsKey("httpHost") ? FormatUtils.getTopPrivateDomain(metaMap.get("httpHost").toString()) : "";
            String subscriberId = metaMap != null && metaMap.containsKey("subscriberId") ? metaMap.get("subscriberId").toString() : "";
            String foundTime = metaMap != null && metaMap.containsKey("foundTime") ? metaMap.get("foundTime").toString() : "0";
            url = URLUtil.normalize(endpointList.get(RandomUtil.randomInt(endpointList.size())) + "/v3/upload?" +
                    "cfg_id=" + policyId +
                    "&file_id=" + fileId +
                    "&file_type=" + fileType +
                    "&found_time=" + foundTime +
                    "&s_ip=" + serverIP +
                    "&s_port=" + serverPort +
                    "&d_ip=" + clientIP +
                    "&d_port=" + clientPort +
                    "&domain=" + domain +
                    "&account=" + subscriberId);
            HttpPost httpPost = new HttpPost(url);
            httpPost.setEntity(new ByteArrayEntity(data));
            executeRequest(httpPost, url);
            chunksOutCounter.inc();
            bytesOutCounter.inc(data.length);
            calculateFileChunkMetrics(fileChunk, fileId);
        } catch (Exception e) {
            LOG.error("post file error. current url: " + url, e);
            errorChunksCounter.inc();
        }
    }

    private void executeRequest(HttpPost httpPost, String url) {
        if (isAsync) {
            asyncHttpClient.execute(httpPost, new FutureCallback<HttpResponse>() {
                @Override
                public void completed(HttpResponse httpResponse) {
                    try {
                        String responseEntity = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
                        if (httpResponse.getStatusLine().getStatusCode() == 200) {
                            if (!responseEntity.contains("\"code\":200")) {
                                LOG.error("post file error. current url: {}, msg: {}", url, responseEntity);
                                errorChunksCounter.inc();
                            }
                        } else {
                            LOG.error("post file error. current url: {}, code: {}, msg: {}", url, httpResponse.getStatusLine().getStatusCode(), responseEntity);
                            errorChunksCounter.inc();
                        }
                    } catch (IOException e) {
                        LOG.error("post file error. current url: " + url, e);
                        errorChunksCounter.inc();
                    }
                }

                @Override
                public void failed(Exception ex) {
                    LOG.error("post file error. current url: " + url, ex);
                    errorChunksCounter.inc();
                }

                @Override
                public void cancelled() {

                }
            });
        } else {
            CloseableHttpResponse response = null;
            try {
                response = syncHttpClient.execute(httpPost);
                String responseEntity = EntityUtils.toString(response.getEntity(), "UTF-8");
                if (response.getStatusLine().getStatusCode() == 200) {
                    if (!responseEntity.contains("\"code\":200")) {
                        LOG.error("post file error. current url: {}, msg: {}", url, responseEntity);
                        errorChunksCounter.inc();
                    }
                } else {
                    LOG.error("post file error. current url: {}, code: {}, msg: {}", url, response.getStatusLine().getStatusCode(), responseEntity);
                    errorChunksCounter.inc();
                }
            } catch (IOException e) {
                LOG.error("post file error. current url: " + url, e);
                errorChunksCounter.inc();
            } finally {
                IoUtil.close(response);
            }
        }
    }

    private void calculateFileChunkMetrics(FileChunk fileChunk, String fileId) {
        String fileType = fileChunk.getFileType();
        long length = fileChunk.getLength();
        calculateChunkSize(length);
        if ("eml".equals(fileType)) {
            emlChunksCounter.inc();
            calculateEmlChunkSize(length);
        } else if ("txt".equals(fileType)) {
            txtChunksCounter.inc();
            calculateTxtChunkSize(length);
        }
        if (fileId.contains("_1")) {
            requestFilesCounter.inc();
        } else if (fileId.contains("_2")) {
            responseFilesCounter.inc();
        }
        if (fileChunk.getOffset() == 0 && fileChunk.getLastChunkFlag() == 1) {
            completeFilesCounter.inc();
            if ("eml".equals(fileType)) {
                completeEmlFilesCounter.inc();
            } else if ("txt".equals(fileType)) {
                completeTxtFilesCounter.inc();
            }
            if (fileChunk.getChunk() == null) {
                nullChunksCounter.inc();
                if ("eml".equals(fileType)) {
                    nullEmlChunksCounter.inc();
                } else if ("txt".equals(fileType)) {
                    nullTxtChunksCounter.inc();
                }
                LOG.info("send file data is null. " + fileChunk.toString());
            }
            if (fileId.contains("_1")) {
                completeRequestFilesCounter.inc();
            } else if (fileId.contains("_2")) {
                completeResponseFilesCounter.inc();
            }
        }
    }

    private void calculateChunkSize(long length) {
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
    }

    private void calculateEmlChunkSize(long length) {
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
    }

    private void calculateTxtChunkSize(long length) {
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
    }
}
