package com.zdjizhi.pojo;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class FileChunk implements Serializable {
    private String uuid;
    private String fileName;
    private String fileType;
    private long offset;
    private long length;
    private byte[] chunk;
    private String combineMode;
    private int lastChunkFlag;
    private int chunkCount;
    private long timestamp;
    private Map<String, Object> meta;
    private String chunkNumbers;

    public FileChunk() {
    }

    public String getChunkNumbers() {
        return chunkNumbers;
    }

    public void setChunkNumbers(String chunkNumbers) {
        this.chunkNumbers = chunkNumbers;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, Object> getMeta() {
        return meta;
    }

    public void setMeta(Map<String, Object> meta) {
        this.meta = meta;
    }

    public int getChunkCount() {
        return chunkCount;
    }

    public void setChunkCount(int chunkCount) {
        this.chunkCount = chunkCount;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public byte[] getChunk() {
        return chunk;
    }

    public void setChunk(byte[] chunk) {
        this.chunk = chunk;
    }

    public String getCombineMode() {
        return combineMode;
    }

    public void setCombineMode(String combineMode) {
        this.combineMode = combineMode;
    }

    public int getLastChunkFlag() {
        return lastChunkFlag;
    }

    public void setLastChunkFlag(int lastChunkFlag) {
        this.lastChunkFlag = lastChunkFlag;
    }

    @Override
    public String toString() {
        return "FileChunk{" +
                "uuid='" + uuid + '\'' +
                ", fileName='" + fileName + '\'' +
                ", fileType='" + fileType + '\'' +
                ", offset=" + offset +
                ", length=" + length +
                ", combineMode='" + combineMode + '\'' +
                ", lastChunkFlag=" + lastChunkFlag +
                ", chunkCount=" + chunkCount +
                ", timestamp=" + timestamp +
                ", meta=" + meta +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileChunk fileChunk = (FileChunk) o;
        return offset == fileChunk.offset &&
                length == fileChunk.length &&
                lastChunkFlag == fileChunk.lastChunkFlag &&
                chunkCount == fileChunk.chunkCount &&
                Objects.equals(uuid, fileChunk.uuid) &&
                Objects.equals(fileName, fileChunk.fileName) &&
                Objects.equals(fileType, fileChunk.fileType) &&
                Arrays.equals(chunk, fileChunk.chunk) &&
                Objects.equals(combineMode, fileChunk.combineMode);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(uuid, fileName, fileType, offset, length, combineMode, lastChunkFlag, chunkCount);
        result = 31 * result + Arrays.hashCode(chunk);
        return result;
    }
}
