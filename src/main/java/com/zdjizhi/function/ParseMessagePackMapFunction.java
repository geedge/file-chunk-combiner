package com.zdjizhi.function;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.pojo.FileChunk;
import org.apache.commons.jexl3.*;
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
    private final boolean enableRateLimit;
    private final long rateLimitThreshold;
    private final String rateLimitExpression;
    public transient Counter parseMessagePacksCounter;
    public transient Counter parseErrorMessagePacksCounter;
    public transient Counter rateLimitDropChunksCounter;
    public transient Counter equal0BChunksCounter;
    public transient Counter lessThan1KBChunksCounter;
    public transient Counter between1KBAnd5KBChunksCounter;
    public transient Counter between5KBAnd10KBChunksCounter;
    public transient Counter between10KBAnd50KBChunksCounter;
    public transient Counter between50KBAnd100KBChunksCounter;
    public transient Counter greaterThan100KBChunksCounter;
    public transient Counter emlChunksCounter;
    public transient Counter txtChunksCounter;
    public transient Counter htmlChunksCounter;
    public transient Counter pcapngChunksCounter;
    public transient Counter mediaChunksCounter;
    private long timestamp;
    private long count;
    private JexlExpression jexlExpression;
    private JexlContext jexlContext;

    public ParseMessagePackMapFunction(boolean enableRateLimit, long rateLimitThreshold, String rateLimitExpression) {
        this.rateLimitThreshold = rateLimitThreshold;
        this.enableRateLimit = enableRateLimit;
        this.rateLimitExpression = rateLimitExpression;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
        equal0BChunksCounter = metricGroup.counter("equal0BChunksCount");
        lessThan1KBChunksCounter = metricGroup.counter("lessThan1KBChunksCount");
        between1KBAnd5KBChunksCounter = metricGroup.counter("between1KBAnd5KBChunksCount");
        between5KBAnd10KBChunksCounter = metricGroup.counter("between5KBAnd10KBChunksCount");
        between10KBAnd50KBChunksCounter = metricGroup.counter("between10KBAnd50KBChunksCount");
        between50KBAnd100KBChunksCounter = metricGroup.counter("between50KBAnd100KBChunksCount");
        greaterThan100KBChunksCounter = metricGroup.counter("greaterThan100KBChunksCount");
        metricGroup.meter("numEqual0BChunksInPerSecond", new MeterView(equal0BChunksCounter));
        metricGroup.meter("numLessThan1KBChunksInPerSecond", new MeterView(lessThan1KBChunksCounter));
        metricGroup.meter("numBetween1KBAnd5KBChunksInPerSecond", new MeterView(between1KBAnd5KBChunksCounter));
        metricGroup.meter("numBetween5KBAnd10KBChunksInPerSecond", new MeterView(between5KBAnd10KBChunksCounter));
        metricGroup.meter("numBetween10KBAnd50KBChunksInPerSecond", new MeterView(between10KBAnd50KBChunksCounter));
        metricGroup.meter("numBetween50KBAnd100KBChunksInPerSecond", new MeterView(between50KBAnd100KBChunksCounter));
        metricGroup.meter("numGreaterThan100KBChunksInPerSecond", new MeterView(greaterThan100KBChunksCounter));
        emlChunksCounter = metricGroup.counter("emlChunksCount");
        txtChunksCounter = metricGroup.counter("txtChunksCount");
        htmlChunksCounter = metricGroup.counter("htmlChunksCount");
        pcapngChunksCounter = metricGroup.counter("pcapngChunksCount");
        mediaChunksCounter = metricGroup.counter("mediaChunksCount");
        metricGroup.meter("numEmlChunksInPerSecond", new MeterView(emlChunksCounter));
        metricGroup.meter("numTxtChunksInPerSecond", new MeterView(txtChunksCounter));
        metricGroup.meter("numHtmlChunksInPerSecond", new MeterView(htmlChunksCounter));
        metricGroup.meter("numPcapngChunksInPerSecond", new MeterView(pcapngChunksCounter));
        metricGroup.meter("numMediaChunksInPerSecond", new MeterView(mediaChunksCounter));
        parseMessagePacksCounter = metricGroup.counter("parseMessagePacksCount");
        parseErrorMessagePacksCounter = metricGroup.counter("parseErrorMessagePacksCount");
        rateLimitDropChunksCounter = metricGroup.counter("rateLimitDropChunksCount");
        metricGroup.meter("numMessagePacksParsePerSecond", new MeterView(parseMessagePacksCounter));
        metricGroup.meter("numMessagePacksParseErrorPerSecond", new MeterView(parseErrorMessagePacksCounter));
        metricGroup.meter("numChunksRateLimitDropPerSecond", new MeterView(rateLimitDropChunksCounter));
        timestamp = System.currentTimeMillis();
        count = 0;
        JexlEngine jexlEngine = new JexlBuilder().create();
        jexlExpression = jexlEngine.createExpression(rateLimitExpression);
        jexlContext = new MapContext();
    }

    @Override
    public FileChunk map(byte[] messagePackData) {
        FileChunk fileChunk = parseMessagePack(messagePackData);
        if (enableRateLimit) {
            count++;
            if (System.currentTimeMillis() - timestamp < 1000 && count > rateLimitThreshold) {
                if (StrUtil.isNotEmpty(rateLimitExpression)) {
                    jexlContext.set(fileChunk.getClass().getSimpleName(), fileChunk);
                    if (Boolean.parseBoolean(jexlExpression.evaluate(jexlContext).toString())) {
                        return fileChunk;
                    }
                }
                rateLimitDropChunksCounter.inc();
                fileChunk = null;
            } else if (System.currentTimeMillis() - timestamp >= 1000) {
                if (StrUtil.isNotEmpty(rateLimitExpression)) {
                    jexlContext.set(fileChunk.getClass().getSimpleName(), fileChunk);
                    if (Boolean.parseBoolean(jexlExpression.evaluate(jexlContext).toString())) {
                        return fileChunk;
                    }
                }
                rateLimitDropChunksCounter.inc();
                fileChunk = null;
                timestamp = System.currentTimeMillis();
                count = 0;
            }
        }
        return fileChunk;
    }

    private FileChunk parseMessagePack(byte[] messagePackData) {
        parseMessagePacksCounter.inc();
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
            calculateChunkSize(fileChunk.getLength());
            calculateFileType(fileChunk.getFileType());
        } catch (Exception e) {
            parseErrorMessagePacksCounter.inc();
            LOG.error("Parse messagePack failed.", e);
            fileChunk = null;
        }
        return fileChunk;
    }

    private void calculateChunkSize(long length) {
        if (length == 0) {
            equal0BChunksCounter.inc();
        } else if (length <= 1024) {
            lessThan1KBChunksCounter.inc();
        } else if (length <= 5 * 1024) {
            between1KBAnd5KBChunksCounter.inc();
        } else if (length <= 10 * 1024) {
            between5KBAnd10KBChunksCounter.inc();
        } else if (length <= 50 * 1024) {
            between10KBAnd50KBChunksCounter.inc();
        } else if (length <= 100 * 1024) {
            between50KBAnd100KBChunksCounter.inc();
        } else {
            greaterThan100KBChunksCounter.inc();
        }
    }

    private void calculateFileType(String fileType) {
        switch (fileType) {
            case "eml":
                emlChunksCounter.inc();
                break;
            case "html":
                htmlChunksCounter.inc();
                break;
            case "txt":
                txtChunksCounter.inc();
                break;
            case "pcapng":
                pcapngChunksCounter.inc();
                break;
            default:
                mediaChunksCounter.inc();
        }
    }
}
