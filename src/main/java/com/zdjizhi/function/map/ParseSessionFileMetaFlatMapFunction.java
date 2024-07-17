package com.zdjizhi.function.map;

import cn.hutool.crypto.digest.DigestUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.fastjson2.JSONObject;
import com.zdjizhi.pojo.FileChunk;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;


public class ParseSessionFileMetaFlatMapFunction extends RichFlatMapFunction<String, FileChunk> {
    private static final Log LOG = LogFactory.get();

    public transient Counter chunksInCounter;
    public transient Counter chunksOutCounter;
    public transient Counter emlFilesCounter;
    public transient Counter errorChunksCounter;
    public transient Counter httpRequestFilesCounter;
    public transient Counter httpResponseFilesCounter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("file_chunk_combiner", "map_session_file_meta");
        chunksInCounter = metricGroup.counter("chunksInCount");
        chunksOutCounter = metricGroup.counter("chunksOutCount");
        errorChunksCounter = metricGroup.counter("errorChunksCount");
        emlFilesCounter = metricGroup.counter("emlFilesInCount");
        httpRequestFilesCounter = metricGroup.counter("httpRequestFilesInCount");
        httpResponseFilesCounter = metricGroup.counter("httpResponseFilesInCount");
        metricGroup.meter("numChunksInPerSecond", new MeterView(chunksInCounter));
        metricGroup.meter("numChunksOutPerSecond", new MeterView(chunksOutCounter));
        metricGroup.meter("numErrorChunksPerSecond", new MeterView(errorChunksCounter));
        metricGroup.meter("numEmlFilesInPerSecond", new MeterView(emlFilesCounter));
        metricGroup.meter("numHttpRequestFilesInPerSecond", new MeterView(httpRequestFilesCounter));
        metricGroup.meter("numHttpResponseFilesInPerSecond", new MeterView(httpResponseFilesCounter));
    }

    @Override
    public void flatMap(String value, Collector<FileChunk> out) {
        try {
            chunksInCounter.inc();
            JSONObject record = JSONObject.parseObject(value);
            if (record.containsKey("security_rule_list") && !record.getJSONArray("security_rule_list").isEmpty()
                    || record.containsKey("monitor_rule_list") && !record.getJSONArray("monitor_rule_list").isEmpty()) {
                if (record.containsKey("http_request_body")) {
                    FileChunk fileChunk = new FileChunk();
                    Map<String, Object> metaMap = new HashMap<>();
                    String uuid = record.getString("http_request_body");
                    fileChunk.setUuid(uuid);
                    metaMap.put("fileId", DigestUtil.md5Hex(uuid) + "_1");
                    metaMap.put("contentType", record.getString("http_request_content_type"));
                    metaMap.put("contentLength", record.getOrDefault("http_request_content_length", 0));
                    metaMap.put("http_session_duration_ms", record.getOrDefault("http_session_duration_ms", 0));
                    getFileMeta(record, metaMap);
                    fileChunk.setMeta(metaMap);
                    out.collect(fileChunk);
                    chunksOutCounter.inc();
                    httpRequestFilesCounter.inc();
                }
                if (record.containsKey("http_response_body")) {
                    FileChunk fileChunk = new FileChunk();
                    Map<String, Object> metaMap = new HashMap<>();
                    String uuid = record.getString("http_response_body");
                    fileChunk.setUuid(uuid);
                    metaMap.put("fileId", DigestUtil.md5Hex(uuid) + "_2");
                    metaMap.put("contentType", record.getString("http_response_content_type"));
                    metaMap.put("contentLength", record.getOrDefault("http_response_content_length", 0));
                    metaMap.put("http_session_duration_ms", record.getOrDefault("http_session_duration_ms", 0));
                    getFileMeta(record, metaMap);
                    fileChunk.setMeta(metaMap);
                    out.collect(fileChunk);
                    chunksOutCounter.inc();
                    httpResponseFilesCounter.inc();
                }
                if (record.containsKey("mail_eml_file")) {
                    FileChunk fileChunk = new FileChunk();
                    Map<String, Object> metaMap = new HashMap<>();
                    String uuid = record.getString("mail_eml_file");
                    fileChunk.setUuid(uuid);
                    metaMap.put("fileId", DigestUtil.md5Hex(uuid) + "_9");
                    getFileMeta(record, metaMap);
                    fileChunk.setMeta(metaMap);
                    out.collect(fileChunk);
                    chunksOutCounter.inc();
                    emlFilesCounter.inc();
                }
            }
        } catch (Exception e) {
            errorChunksCounter.inc();
            LOG.error("Parse session file meta failed.", e);
        }
    }

    private void getFileMeta(JSONObject record, Map<String, Object> metaMap) {
        metaMap.put("policyId", record.containsKey("monitor_rule_list") && !record.getJSONArray("monitor_rule_list").isEmpty() ? record.getJSONArray("monitor_rule_list").getInteger(0) : 0);
        metaMap.put("serverIP", record.getString("server_ip"));
        metaMap.put("serverPort", record.getInteger("server_port"));
        metaMap.put("clientIP", record.getString("client_ip"));
        metaMap.put("clientPort", record.getInteger("client_port"));
        metaMap.put("httpHost", record.getOrDefault("http_host", "").toString());
        metaMap.put("subscriberId", record.getOrDefault("subscriber_id", "").toString());
        metaMap.put("foundTime", record.getLong("end_timestamp_ms"));
        metaMap.put("sled_ip", record.getOrDefault("sled_ip", "").toString());
        metaMap.put("duration_ms", record.getInteger("duration_ms"));
    }
}
