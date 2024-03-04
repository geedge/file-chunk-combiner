package com.zdjizhi.sink;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.*;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.config.Configs;
import com.zdjizhi.pojo.FileChunk;
import com.zdjizhi.utils.HttpClientUtil;
import com.zdjizhi.utils.PublicUtil;
import org.apache.commons.lang.CharEncoding;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
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

import static com.zdjizhi.utils.HttpHeaderConstants.*;
import static com.zdjizhi.utils.PublicConstants.*;

public class HosSink extends RichSinkFunction<FileChunk> {
    private static final Log LOG = LogFactory.get();

    private final Configuration configuration;
    private transient Counter sendHosCounter;
    private transient Counter sendHosErrorCounter;
    private transient Counter sendHosFileCounter;
    private transient Counter sendHosChunkCounter;
    private boolean isAsync;
    private CloseableHttpClient syncHttpClient;
    private CloseableHttpAsyncClient asyncHttpClient;
    private int loadBalanceMode;
    private volatile String endpoint;
    private List<String> ipList;
    private List<String> portList;
    private String token;
    private volatile String bathPutUrl;
    private HashMap<String, String> hosMessage;
    private String objectsMeta = "";
    private String objectsOffset = "";
    private List<byte[]> byteList;
    private long maxBatchSize;
    private long maxBatchCount;
    private long chunkSize = 0;
    private int chunkCount = 0;

    public HosSink(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
        sendHosCounter = metricGroup.counter("sendHosCount");
        sendHosErrorCounter = metricGroup.counter("sendHosErrorCount");
        sendHosFileCounter = metricGroup.counter("sendHosFileCount");
        sendHosChunkCounter = metricGroup.counter("sendHosChunkCount");
        loadBalanceMode = configuration.getInteger(Configs.SINK_HOS_LOAD_BALANCE_MODE);
        if (loadBalanceMode == 0) {
            endpoint = configuration.getString(Configs.SINK_HOS_ENDPOINT);
        } else if (loadBalanceMode == 1) {
            String[] ipPortArr = configuration.get(Configs.SINK_HOS_ENDPOINT).split(":");
            ipList = Arrays.asList(ipPortArr[0].split(","));
            portList = Arrays.asList(ipPortArr[1].split(","));
            endpoint = ipList.get(RandomUtil.randomInt(ipList.size())) + ":" + portList.get(RandomUtil.randomInt(portList.size()));
        }
        token = configuration.get(Configs.SINK_HOS_TOKEN);
        isAsync = configuration.getBoolean(Configs.SINK_ASYNC);
        if (isAsync) {
            asyncHttpClient = HttpClientUtil.getInstance(configuration).getAsyncHttpClient();
            asyncHttpClient.start();
        } else {
            syncHttpClient = HttpClientUtil.getInstance(configuration).getSyncHttpClient();
        }
        bathPutUrl = URLUtil.normalize(endpoint + "/hos/" + configuration.get(Configs.SINK_HOS_BUCKET) + "/" + PublicUtil.getUUID()) + "?multiFile";
        maxBatchSize = configuration.getLong(Configs.SINK_BATCH_SIZE);
        maxBatchCount = configuration.getInteger(Configs.SINK_BATCH_COUNT);
        hosMessage = new HashMap<>();
        objectsMeta = "";
        objectsOffset = "";
        byteList = new ArrayList<>();
    }

    @Override
    public void invoke(FileChunk fileChunk, Context context) {
        byte[] data = "".getBytes();
        if (fileChunk.getChunk() != null) {
            data = fileChunk.getChunk();
        }
        sendHosChunkCounter.inc();
        if (configuration.get(Configs.SINK_BATCH)) {
            hosMessage.put(HOS_META_FILE_TYPE, fileChunk.getFileType());
            hosMessage.put(HOS_META_FILENAME, fileChunk.getUuid());
            if (COMBINE_MODE_SEEK.equals(fileChunk.getCombineMode())) {
                hosMessage.put(HOS_OFFSET, fileChunk.getOffset() + "");
                hosMessage.put(HOS_PART_LAST_FLAG, fileChunk.getLastChunkFlag() + "");
                if (fileChunk.getOffset() == 0) {
                    sendHosFileCounter.inc();
                }
            } else {
                hosMessage.put(HOS_PART_NUMBER, fileChunk.getTimestamp() + "");
                hosMessage.put(HOS_PART_CHUNK_NUMBERS, fileChunk.getChunkNumbers());
            }
            hosMessage.put(HOS_PART_CHUNK_COUNT, fileChunk.getChunkCount() + "");
            Map<String, Object> metaMap = fileChunk.getMeta();
            if (metaMap != null && metaMap.size() > 0) {
                for (String meta : metaMap.keySet()) {
                    hosMessage.put(HOS_META_PREFIX + StrUtil.toSymbolCase(meta, CharUtil.DASHED), metaMap.get(meta) + "");
                }
            }
            objectsMeta += hosMessage.toString() + ";";
            hosMessage.clear();
            objectsOffset += data.length + ";";
            byteList.add(data);
            chunkCount++;
            chunkSize += data.length;
            if (chunkSize >= maxBatchSize || chunkCount >= maxBatchCount) {
                HttpPut httpPut = new HttpPut(bathPutUrl);
                httpPut.setHeader(TOKEN, token);
                httpPut.setHeader(HOS_UPLOAD_TYPE, UPLOAD_TYPE_APPENDV2);
                httpPut.setHeader(HOS_COMBINE_MODE, fileChunk.getCombineMode());
                httpPut.setHeader(HOS_OBJECTS_META, objectsMeta);
                httpPut.setHeader(HOS_OBJECTS_OFFSET, objectsOffset);
                byte[][] bytes = new byte[byteList.size()][];
                byteList.toArray(bytes);
                byte[] newData = ArrayUtil.addAll(bytes);
                httpPut.setEntity(new ByteArrayEntity(newData));
                byteList.clear();
                executeRequest(httpPut);
                objectsMeta = "";
                objectsOffset = "";
                chunkSize = 0;
                chunkCount = 0;
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
                if (fileChunk.getOffset() == 0) {
                    sendHosFileCounter.inc();
                }
            } else {
                httpPut.setHeader(HOS_PART_NUMBER, fileChunk.getTimestamp() + "");
                httpPut.setHeader(HOS_PART_CHUNK_NUMBERS, fileChunk.getChunkNumbers());
            }
            httpPut.setHeader(HOS_PART_CHUNK_COUNT, fileChunk.getChunkCount() + "");
            Map<String, Object> metaMap = fileChunk.getMeta();
            if (metaMap != null && metaMap.size() > 0) {
                for (String meta : metaMap.keySet()) {
                    httpPut.setHeader(HOS_META_PREFIX + StrUtil.toSymbolCase(meta, CharUtil.DASHED), metaMap.get(meta) + "");
                }
            }
            httpPut.setEntity(new ByteArrayEntity(data));
            executeRequest(httpPut);
        }
    }

    @Override
    public void close() {
        IoUtil.close(syncHttpClient);
        IoUtil.close(asyncHttpClient);
    }

    private void executeRequest(HttpPut httpPut) {
        sendHosCounter.inc();
        if (isAsync) {
            asyncHttpClient.execute(httpPut, new FutureCallback<HttpResponse>() {
                @Override
                public void completed(HttpResponse httpResponse) {
                    try {
                        if (httpResponse.getStatusLine().getStatusCode() != 200) {
                            String responseEntity = EntityUtils.toString(httpResponse.getEntity(), CharEncoding.UTF_8);
                            LOG.error("put part to hos error. code: " + httpResponse.getStatusLine().getStatusCode() + ". message: " + responseEntity);
                            sendHosErrorCounter.inc();
                        }
                    } catch (IOException e) {
                        LOG.error("put part to hos error.", e);
                        sendHosErrorCounter.inc();
                    }
                }

                @Override
                public void failed(Exception ex) {
                    LOG.error("put part to hos error.", ex);
                    sendHosErrorCounter.inc();
                    if (loadBalanceMode == 1 && ex instanceof ConnectException) {
                        endpoint = ipList.get(RandomUtil.randomInt(ipList.size())) + ":" + portList.get(RandomUtil.randomInt(portList.size()));
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
                    String responseEntity = EntityUtils.toString(response.getEntity(), CharEncoding.UTF_8);
                    LOG.error("put part to hos error. code: " + response.getStatusLine().getStatusCode() + ". message: " + responseEntity);
                    sendHosErrorCounter.inc();
                }
            } catch (IOException e) {
                LOG.error("put part to hos error.", e);
                sendHosErrorCounter.inc();
                if (loadBalanceMode == 1 && (e instanceof HttpHostConnectException || e instanceof ConnectTimeoutException)) {
                    endpoint = ipList.get(RandomUtil.randomInt(ipList.size())) + ":" + portList.get(RandomUtil.randomInt(portList.size()));
                }
            } finally {
                IoUtil.close(response);
            }
        }
    }
}
