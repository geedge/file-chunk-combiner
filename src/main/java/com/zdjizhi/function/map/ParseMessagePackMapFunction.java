package com.zdjizhi.function.map;

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

    public transient Counter chunksInCounter;
    public transient Counter chunksOutCounter;
    public transient Counter bytesInCounter;
    public transient Counter bytesOutCounter;
    public transient Counter errorChunksCounter;
    public transient Counter lessThan1KBChunksCounter;
    public transient Counter between1KBAnd3KBChunksCounter;
    public transient Counter between3KBAnd5KBChunksCounter;
    public transient Counter between5KBAnd10KBChunksCounter;
    public transient Counter between10KBAnd50KBChunksCounter;
    public transient Counter between50KBAnd100KBChunksCounter;
    public transient Counter greaterThan100KBChunksCounter;
    public transient Counter offsetLessThan10KBChunksCounter;
    public transient Counter offsetBetween10KBAnd100KBChunksCounter;
    public transient Counter offsetBetween100KBAnd1MBChunksCounter;
    public transient Counter offsetBetween1MBAnd10MBChunksCounter;
    public transient Counter offsetBetween10MBAnd100MBChunksCounter;
    public transient Counter offsetBetween100MBAnd1GBChunksCounter;
    public transient Counter offsetGreaterThan1GBChunksCounter;
    public transient Counter lessThan10KBFilesCounter;
    public transient Counter between10KBAnd100KBFilesCounter;
    public transient Counter between100KBAnd1MBFilesCounter;
    public transient Counter between1MBAnd10MBFilesCounter;
    public transient Counter between10MBAnd100MBFilesCounter;
    public transient Counter between100MBAnd1GBFilesCounter;
    public transient Counter greaterThan1GBFilesCounter;
    public transient Counter lessThan10KBEmlChunksCounter;
    public transient Counter between10KBAnd100KBEmlChunksCounter;
    public transient Counter between100KBAnd1MBEmlChunksCounter;
    public transient Counter between1MBAnd10MBEmlChunksCounter;
    public transient Counter greaterThan10MBEmlChunksCounter;
    public transient Counter lessThan10KBTxtChunksCounter;
    public transient Counter between10KBAnd100KBTxtChunksCounter;
    public transient Counter between100KBAnd1MBTxtChunksCounter;
    public transient Counter between1MBAnd10MBTxtChunksCounter;
    public transient Counter greaterThan10MBTxtChunksCounter;
    public transient Counter emlChunksCounter;
    public transient Counter txtChunksCounter;
    public transient Counter htmlChunksCounter;
    public transient Counter pcapngChunksCounter;
    public transient Counter mediaChunksCounter;
    public transient Counter startChunksCounter;
    public transient Counter endChunksCounter;
    public transient Counter nullChunksCounter;
    public transient Counter nullTxtChunksCounter;
    public transient Counter nullEmlChunksCounter;
    public transient Counter emlFilesCounter;
    public transient Counter txtFilesCounter;
    public transient Counter htmlFilesCounter;
    public transient Counter pcapngFilesCounter;
    public transient Counter mediaFilesCounter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("file_chunk_combiner", "map_message_pack");
        lessThan1KBChunksCounter = metricGroup.counter("lessThan1KBChunksCount");
        between1KBAnd3KBChunksCounter = metricGroup.counter("between1KBAnd3KBChunksCount");
        between3KBAnd5KBChunksCounter = metricGroup.counter("between3KBAnd5KBChunksCount");
        between5KBAnd10KBChunksCounter = metricGroup.counter("between5KBAnd10KBChunksCount");
        between10KBAnd50KBChunksCounter = metricGroup.counter("between10KBAnd50KBChunksCount");
        between50KBAnd100KBChunksCounter = metricGroup.counter("between50KBAnd100KBChunksCount");
        greaterThan100KBChunksCounter = metricGroup.counter("greaterThan100KBChunksCount");
        metricGroup.meter("numLessThan1KBChunksInPerSecond", new MeterView(lessThan1KBChunksCounter));
        metricGroup.meter("numBetween1KBAnd3KBChunksInPerSecond", new MeterView(between1KBAnd3KBChunksCounter));
        metricGroup.meter("numBetween3KBAnd5KBChunksInPerSecond", new MeterView(between3KBAnd5KBChunksCounter));
        metricGroup.meter("numBetween5KBAnd10KBChunksInPerSecond", new MeterView(between5KBAnd10KBChunksCounter));
        metricGroup.meter("numBetween10KBAnd50KBChunksInPerSecond", new MeterView(between10KBAnd50KBChunksCounter));
        metricGroup.meter("numBetween50KBAnd100KBChunksInPerSecond", new MeterView(between50KBAnd100KBChunksCounter));
        metricGroup.meter("numGreaterThan100KBChunksInPerSecond", new MeterView(greaterThan100KBChunksCounter));
        offsetLessThan10KBChunksCounter = metricGroup.counter("offsetLessThan10KBChunksCount");
        offsetBetween10KBAnd100KBChunksCounter = metricGroup.counter("offsetBetween10KBAnd100KBChunksCount");
        offsetBetween100KBAnd1MBChunksCounter = metricGroup.counter("offsetBetween100KBAnd1MBChunksCount");
        offsetBetween1MBAnd10MBChunksCounter = metricGroup.counter("offsetBetween1MBAnd10MBChunksCount");
        offsetBetween10MBAnd100MBChunksCounter = metricGroup.counter("offsetBetween10MBAnd100MBChunksCount");
        offsetBetween100MBAnd1GBChunksCounter = metricGroup.counter("offsetBetween100MBAnd1GBChunksCount");
        offsetGreaterThan1GBChunksCounter = metricGroup.counter("offsetGreaterThan1GBChunksCount");
        metricGroup.meter("numOffsetLessThan10KBChunksInPerSecond", new MeterView(offsetLessThan10KBChunksCounter));
        metricGroup.meter("numOffsetBetween10KBAnd100KBChunksInPerSecond", new MeterView(offsetBetween10KBAnd100KBChunksCounter));
        metricGroup.meter("numOffsetBetween100KBAnd1MBChunksInPerSecond", new MeterView(offsetBetween100KBAnd1MBChunksCounter));
        metricGroup.meter("numOffsetBetween1MBAnd10MBChunksInPerSecond", new MeterView(offsetBetween1MBAnd10MBChunksCounter));
        metricGroup.meter("numOffsetBetween10MBAnd100MBChunksInPerSecond", new MeterView(offsetBetween10MBAnd100MBChunksCounter));
        metricGroup.meter("numOffsetBetween100MBAnd1GBChunksInPerSecond", new MeterView(offsetBetween100MBAnd1GBChunksCounter));
        metricGroup.meter("numOffsetGreaterThan1GBChunksInPerSecond", new MeterView(offsetGreaterThan1GBChunksCounter));
        lessThan10KBFilesCounter = metricGroup.counter("lessThan10KBFilesCount");
        between10KBAnd100KBFilesCounter = metricGroup.counter("between10KBAnd100KBFilesCount");
        between100KBAnd1MBFilesCounter = metricGroup.counter("between100KBAnd1MBFilesCount");
        between1MBAnd10MBFilesCounter = metricGroup.counter("between1MBAnd10MBFilesCount");
        between10MBAnd100MBFilesCounter = metricGroup.counter("between10MBAnd100MBFilesCount");
        between100MBAnd1GBFilesCounter = metricGroup.counter("between100MBAnd1GBFilesCount");
        greaterThan1GBFilesCounter = metricGroup.counter("greaterThan1GBFilesCount");
        metricGroup.meter("numLessThan10KBFilesInPerSecond", new MeterView(lessThan10KBFilesCounter));
        metricGroup.meter("numBetween10KBAnd100KBFilesInPerSecond", new MeterView(between10KBAnd100KBFilesCounter));
        metricGroup.meter("numBetween100KBAnd1MBFilesInPerSecond", new MeterView(between100KBAnd1MBFilesCounter));
        metricGroup.meter("numBetween1MBAnd10MBFilesInPerSecond", new MeterView(between1MBAnd10MBFilesCounter));
        metricGroup.meter("numBetween10MBAnd100MBFilesInPerSecond", new MeterView(between10MBAnd100MBFilesCounter));
        metricGroup.meter("numBetween100MBAnd1GBFilesInPerSecond", new MeterView(between100MBAnd1GBFilesCounter));
        metricGroup.meter("numGreaterThan1GBFilesInPerSecond", new MeterView(greaterThan1GBFilesCounter));
        lessThan10KBEmlChunksCounter = metricGroup.counter("lessThan10KBEmlChunksCount");
        between10KBAnd100KBEmlChunksCounter = metricGroup.counter("between10KBAnd100KBEmlChunksCount");
        between100KBAnd1MBEmlChunksCounter = metricGroup.counter("between100KBAnd1MBEmlChunksCount");
        between1MBAnd10MBEmlChunksCounter = metricGroup.counter("between1MBAnd10MBEmlChunksCount");
        greaterThan10MBEmlChunksCounter = metricGroup.counter("greaterThan10MBEmlChunksCount");
        metricGroup.meter("numLessThan10KBEmlFilesInPerSecond", new MeterView(lessThan10KBEmlChunksCounter));
        metricGroup.meter("numBetween10KBAnd100KBEmlFilesInPerSecond", new MeterView(between10KBAnd100KBEmlChunksCounter));
        metricGroup.meter("numBetween100KBAnd1MBEmlFilesInPerSecond", new MeterView(between100KBAnd1MBEmlChunksCounter));
        metricGroup.meter("numBetween1MBAnd10MBEmlFilesInPerSecond", new MeterView(between1MBAnd10MBEmlChunksCounter));
        metricGroup.meter("numGreaterThan10MBEmlFilesInPerSecond", new MeterView(greaterThan10MBEmlChunksCounter));
        lessThan10KBTxtChunksCounter = metricGroup.counter("lessThan10KBTxtChunksCount");
        between10KBAnd100KBTxtChunksCounter = metricGroup.counter("between10KBAnd100KBTxtChunksCount");
        between100KBAnd1MBTxtChunksCounter = metricGroup.counter("between100KBAnd1MBTxtChunksCount");
        between1MBAnd10MBTxtChunksCounter = metricGroup.counter("between1MBAnd10MBTxtChunksCount");
        greaterThan10MBTxtChunksCounter = metricGroup.counter("greaterThan10MBTxtChunksCount");
        metricGroup.meter("numLessThan10KBTxtFilesInPerSecond", new MeterView(lessThan10KBTxtChunksCounter));
        metricGroup.meter("numBetween10KBAnd100KBTxtFilesInPerSecond", new MeterView(between10KBAnd100KBTxtChunksCounter));
        metricGroup.meter("numBetween100KBAnd1MBTxtFilesInPerSecond", new MeterView(between100KBAnd1MBTxtChunksCounter));
        metricGroup.meter("numBetween1MBAnd10MBTxtFilesInPerSecond", new MeterView(between1MBAnd10MBTxtChunksCounter));
        metricGroup.meter("numGreaterThan10MBTxtFilesInPerSecond", new MeterView(greaterThan10MBTxtChunksCounter));
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
        startChunksCounter = metricGroup.counter("startChunksCount");
        endChunksCounter = metricGroup.counter("endChunksCount");
        metricGroup.meter("numStartChunksInPerSecond", new MeterView(startChunksCounter));
        metricGroup.meter("numEndChunksInPerSecond", new MeterView(endChunksCounter));
        nullChunksCounter = metricGroup.counter("nullChunksCount");
        nullEmlChunksCounter = metricGroup.counter("nullTxtChunksCount");
        nullTxtChunksCounter = metricGroup.counter("nullEmlChunksCount");
        metricGroup.meter("numNullFilesInPerSecond", new MeterView(nullChunksCounter));
        metricGroup.meter("numNullEmlFilesInPerSecond", new MeterView(nullEmlChunksCounter));
        metricGroup.meter("numNullTxtFilesInPerSecond", new MeterView(nullTxtChunksCounter));
        emlFilesCounter = metricGroup.counter("emlFilesCount");
        txtFilesCounter = metricGroup.counter("txtFilesCount");
        htmlFilesCounter = metricGroup.counter("htmlFilesCount");
        pcapngFilesCounter = metricGroup.counter("pcapngFilesCount");
        mediaFilesCounter = metricGroup.counter("mediaFilesCount");
        metricGroup.meter("numEmlFilesInPerSecond", new MeterView(emlFilesCounter));
        metricGroup.meter("numTxtFilesInPerSecond", new MeterView(txtFilesCounter));
        metricGroup.meter("numHtmlFilesInPerSecond", new MeterView(htmlFilesCounter));
        metricGroup.meter("numPcapngFilesInPerSecond", new MeterView(pcapngFilesCounter));
        metricGroup.meter("numMediaFilesInPerSecond", new MeterView(mediaFilesCounter));
        chunksInCounter = metricGroup.counter("chunksInCount");
        chunksOutCounter = metricGroup.counter("chunksOutCount");
        bytesInCounter = metricGroup.counter("bytesInCount");
        bytesOutCounter = metricGroup.counter("bytesOutCount");
        errorChunksCounter = metricGroup.counter("errorChunksCount");
        metricGroup.meter("numChunksInPerSecond", new MeterView(chunksInCounter));
        metricGroup.meter("numChunksOutPerSecond", new MeterView(chunksOutCounter));
        metricGroup.meter("numBytesInPerSecond", new MeterView(bytesInCounter));
        metricGroup.meter("numBytesOutPerSecond", new MeterView(bytesOutCounter));
        metricGroup.meter("numErrorChunksPerSecond", new MeterView(errorChunksCounter));
    }

    @Override
    public FileChunk map(byte[] messagePackData) {
        return parseMessagePack(messagePackData);
    }

    private FileChunk parseMessagePack(byte[] messagePackData) {
        chunksInCounter.inc();
        bytesInCounter.inc(messagePackData.length);
        FileChunk fileChunk;
        try (MessageUnpacker messageUnpacker = MessagePack.newDefaultUnpacker(messagePackData)) {
            fileChunk = new FileChunk();
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
                        metaJsonObject.keySet().forEach(key -> metaMap.put(key, metaJsonObject.get(key)));
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
            chunksOutCounter.inc();
            bytesOutCounter.inc(fileChunk.getLength());
            calculateFileChunkMetrics(fileChunk);
        } catch (Exception e) {
            errorChunksCounter.inc();
            LOG.error("Parse messagePack failed.", e);
            fileChunk = null;
        }
        return fileChunk;
    }

    private void calculateFileChunkMetrics(FileChunk fileChunk) {
        String fileType = fileChunk.getFileType();
        long length = fileChunk.getLength();
        long offset = fileChunk.getOffset();
        int lastChunkFlag = fileChunk.getLastChunkFlag();
        if (length <= 1024) {
            lessThan1KBChunksCounter.inc();
        } else if (length <= 3 * 1024) {
            between1KBAnd3KBChunksCounter.inc();
        } else if (length <= 5 * 1024) {
            between3KBAnd5KBChunksCounter.inc();
        } else if (length <= 10 * 1024) {
            between5KBAnd10KBChunksCounter.inc();
        } else if (length <= 50 * 1024) {
            between10KBAnd50KBChunksCounter.inc();
        } else if (length <= 100 * 1024) {
            between50KBAnd100KBChunksCounter.inc();
        } else {
            greaterThan100KBChunksCounter.inc();
        }
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
        if (offset == 0 && lastChunkFlag == 1 && fileChunk.getChunk() == null) {
            nullChunksCounter.inc();
            if ("eml".equals(fileType)) {
                nullEmlChunksCounter.inc();
            } else if ("txt".equals(fileType)) {
                nullTxtChunksCounter.inc();
            }
            LOG.info("map file data is null: " + fileChunk.toString());
        }
        if (offset == 0) {
            startChunksCounter.inc();
            switch (fileType) {
                case "eml":
                    emlFilesCounter.inc();
                    break;
                case "html":
                    htmlFilesCounter.inc();
                    break;
                case "txt":
                    txtFilesCounter.inc();
                    break;
                case "pcapng":
                    pcapngFilesCounter.inc();
                    break;
                default:
                    mediaFilesCounter.inc();
            }
        }
        if (lastChunkFlag == 1) {
            endChunksCounter.inc();
            if (offset <= 10 * 1024) {
                lessThan10KBFilesCounter.inc();
            } else if (offset <= 100 * 1024) {
                between10KBAnd100KBFilesCounter.inc();
            } else if (offset <= 1024 * 1024) {
                between100KBAnd1MBFilesCounter.inc();
            } else if (offset <= 10 * 1024 * 1024) {
                between1MBAnd10MBFilesCounter.inc();
            } else if (offset <= 100 * 1024 * 1024) {
                between10MBAnd100MBFilesCounter.inc();
            } else if (offset <= 1024 * 1024 * 1024) {
                between100MBAnd1GBFilesCounter.inc();
            } else {
                greaterThan1GBFilesCounter.inc();
            }
            if ("eml".equals(fileType)) {
                calculateEmlFileSize(offset);
            }
            if ("txt".equals(fileType)) {
                calculateTxtFileSize(offset);
            }
        }
        if (offset <= 10 * 1024) {
            offsetLessThan10KBChunksCounter.inc();
        } else if (offset <= 100 * 1024) {
            offsetBetween10KBAnd100KBChunksCounter.inc();
        } else if (offset <= 1024 * 1024) {
            offsetBetween100KBAnd1MBChunksCounter.inc();
        } else if (offset <= 10 * 1024 * 1024) {
            offsetBetween1MBAnd10MBChunksCounter.inc();
        } else if (offset <= 100 * 1024 * 1024) {
            offsetBetween10MBAnd100MBChunksCounter.inc();
        } else if (offset <= 1024 * 1024 * 1024) {
            offsetBetween100MBAnd1GBChunksCounter.inc();
        } else {
            offsetGreaterThan1GBChunksCounter.inc();
        }
    }

    private void calculateEmlFileSize(long length) {
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

    private void calculateTxtFileSize(long length) {
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
