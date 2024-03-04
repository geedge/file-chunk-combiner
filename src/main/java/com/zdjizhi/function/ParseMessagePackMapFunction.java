package com.zdjizhi.function;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.pojo.FileChunk;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.util.HashMap;
import java.util.Map;

public class ParseMessagePackMapFunction extends RichMapFunction<byte[], FileChunk> {
    private static final Log LOG = LogFactory.get();

    @Override
    public FileChunk map(byte[] messagePackData) {
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
            if ("append".equals(fileChunk.getCombineMode())) {
                fileChunk.setLastChunkFlag(0);
            }
        } catch (Exception e) {
            LOG.error("Parse messagePack failed.", e);
            fileChunk = null;
        }
        return fileChunk;
    }
}
