package com.zdjizhi.function;

import com.zdjizhi.pojo.FileChunk;
import org.apache.flink.api.java.functions.KeySelector;

public class FileChunkKeySelector implements KeySelector<FileChunk, String> {
    @Override
    public String getKey(FileChunk value) {
        return value.getUuid();
    }
}
