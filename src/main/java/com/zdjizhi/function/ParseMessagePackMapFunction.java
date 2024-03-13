package com.zdjizhi.function;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.pojo.FileChunk;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.*;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.util.HashMap;
import java.util.Map;

import static com.zdjizhi.utils.PublicConstants.COMBINE_MODE_APPEND;

public class ParseMessagePackMapFunction extends RichMapFunction<byte[], FileChunk> {
    private static final Log LOG = LogFactory.get();

    public transient Counter parseMessagePackCounter;
    public transient Counter parseMessagePackErrorCounter;
    public transient Counter rateLimitDropCounter;
    private final boolean enableRateLimit;
    private final long rateLimitThreshold;
    private long timestamp;
    private long count;

    public ParseMessagePackMapFunction(boolean enableRateLimit, long rateLimitThreshold) {
        this.rateLimitThreshold = rateLimitThreshold;
        this.enableRateLimit = enableRateLimit;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
        parseMessagePackCounter = metricGroup.counter("parseMessagePackCount");
        parseMessagePackErrorCounter = metricGroup.counter("parseMessagePackErrorCount");
        rateLimitDropCounter = metricGroup.counter("rateLimitDropCount");
        metricGroup.meter("parseMessagePackPerSecond", new MeterView(parseMessagePackCounter));
        metricGroup.meter("parseMessagePackErrorPerSecond", new MeterView(parseMessagePackErrorCounter));
        metricGroup.meter("rateLimitDropPerSecond", new MeterView(rateLimitDropCounter));
        timestamp = System.currentTimeMillis();
        count = 0;
    }

    @Override
    public FileChunk map(byte[] messagePackData) {
        FileChunk fileChunk = null;
        if (enableRateLimit) {
            count++;
            if (System.currentTimeMillis() - timestamp < 1000 && count <= rateLimitThreshold) {
                fileChunk = parseMessagePack(messagePackData);
            } else if (System.currentTimeMillis() - timestamp < 1000 && count > rateLimitThreshold) {
                rateLimitDropCounter.inc();
            } else {
                rateLimitDropCounter.inc();
                timestamp = System.currentTimeMillis();
                count = 0;
            }
        } else {
            fileChunk = parseMessagePack(messagePackData);
        }
        return fileChunk;
    }

    private FileChunk parseMessagePack(byte[] messagePackData) {
        parseMessagePackCounter.inc();
        FileChunk fileChunk;
        try {
            fileChunk = new FileChunk();
            MessageUnpacker messageUnpacker = MessagePack.newDefaultUnpacker(messagePackData);
            int numFields = messageUnpacker.unpackMapHeader();
            Map<String, Object> metaMap = new HashMap<>();
            for (int i = 0; i < numFields; i++) {
                String fieldName = messageUnpacker.unpackString();
                switch (fieldName) {
                    case "uuid":
                        fileChunk.setUuid(messageUnpacker.unpackString());
                        break;
                    case "fileName":
                        fileChunk.setFileName(messageUnpacker.unpackString());
                        break;
                    case "fileType":
                        fileChunk.setFileType(messageUnpacker.unpackString());
                        break;
                    case "combineMode":
                        fileChunk.setCombineMode(messageUnpacker.unpackString());
                        break;
                    case "offset":
                        fileChunk.setOffset(messageUnpacker.unpackLong());
                        break;
                    case "length":
                        fileChunk.setLength(messageUnpacker.unpackLong());
                        break;
                    case "lastChunkFlag":
                        fileChunk.setLastChunkFlag(messageUnpacker.unpackInt());
                        break;
                    case "chunk":
                        fileChunk.setChunk(messageUnpacker.readPayload(messageUnpacker.unpackRawStringHeader()));
                        break;
                    case "timestamp":
                        fileChunk.setTimestamp(messageUnpacker.unpackLong());
                        break;
                    case "meta":
                        String meta = messageUnpacker.unpackString();
                        JSONObject metaJsonObject = JSONUtil.parseObj(meta);
                        for (String key : metaJsonObject.keySet()) {
                            metaMap.put(key, metaJsonObject.get(key));
                        }
                        fileChunk.setMeta(metaMap);
                        break;
                    default:
                        messageUnpacker.skipValue();
                        break;
                }
            }
            if (COMBINE_MODE_APPEND.equals(fileChunk.getCombineMode())) {
                fileChunk.setLastChunkFlag(0);
            }
        } catch (Exception e) {
            parseMessagePackErrorCounter.inc();
            LOG.error("Parse messagePack failed.", e);
            fileChunk = null;
        }
        return fileChunk;
    }
}
