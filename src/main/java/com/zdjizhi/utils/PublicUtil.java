package com.zdjizhi.utils;

import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.digest.DigestUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.pojo.FileChunk;
import org.apache.flink.metrics.Counter;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.zdjizhi.utils.PublicConstants.COMBINE_MODE_SEEK;

public class PublicUtil {
    private static final Log LOG = LogFactory.get();

    public static List<FileChunk> combine(Iterable<FileChunk> input, long keyMaxChunk, Counter duplicateChunkCounter, Counter combineErrorCounter, Counter seekChunkCounter, Counter appendChunkCounter) {
        List<FileChunk> combinedFileChunkList = new ArrayList<>();
        try {
            List<FileChunk> originalFileChunkList = StreamSupport.stream(input.spliterator(), false).collect(Collectors.toList());
            System.out.println(originalFileChunkList);
            List<byte[]> waitingToCombineChunkList = new ArrayList<>();
            if (COMBINE_MODE_SEEK.equals(originalFileChunkList.get(0).getCombineMode())) {
                seekChunkCounter.inc();
                // 按照offset排序
                originalFileChunkList.sort(Comparator.comparingLong(FileChunk::getOffset));
                Iterator<FileChunk> originalFileChunkIterator = originalFileChunkList.iterator();
                if (originalFileChunkIterator.hasNext()) {
                    int duplicateCount = 0;
                    FileChunk currentFileChunk = originalFileChunkIterator.next();
                    int lastChunkFlag = currentFileChunk.getLastChunkFlag();
                    long startOffset = currentFileChunk.getOffset();
                    if (currentFileChunk.getChunk() != null && currentFileChunk.getChunk().length > 0) {
                        waitingToCombineChunkList.add(currentFileChunk.getChunk());
                    }
                    while (originalFileChunkIterator.hasNext()) {
                        seekChunkCounter.inc();
                        long expectedOffset = currentFileChunk.getOffset() + currentFileChunk.getLength();
                        currentFileChunk = originalFileChunkIterator.next();
                        long actualOffset = currentFileChunk.getOffset();
                        if (expectedOffset > actualOffset) {// 期望offset大于当前offset，该块为重复块，跳过该块
                            duplicateCount++;
                        } else if (expectedOffset == actualOffset) {// 期望offset等于当前offset，将该块添加到待合并集合中
                            if (currentFileChunk.getLastChunkFlag() == 1) {
                                lastChunkFlag = currentFileChunk.getLastChunkFlag();
                            }
                            if (currentFileChunk.getChunk() != null && currentFileChunk.getChunk().length > 0) {
                                waitingToCombineChunkList.add(currentFileChunk.getChunk());
                            }
                        } else {// 期望offset小于当前offset，说明缺块
                            if (waitingToCombineChunkList.size() > 0) {//将可合并的chunk合并，清空集合
                                combinedFileChunkList.add(combineChunk(waitingToCombineChunkList, currentFileChunk.getUuid(), currentFileChunk.getFileName(), currentFileChunk.getFileType(), startOffset, currentFileChunk.getCombineMode(), lastChunkFlag, originalFileChunkList.get(0).getMeta(), 0, null));
                                waitingToCombineChunkList.clear();
                            } else {
                                if (lastChunkFlag == 1) {
                                    combinedFileChunkList.add(currentFileChunk);
                                }
                            }
                            // 将当前块作为第一个块，继续合并
                            startOffset = currentFileChunk.getOffset();// 重置起始offset
                            lastChunkFlag = currentFileChunk.getLastChunkFlag();
                            if (currentFileChunk.getChunk() != null && currentFileChunk.getChunk().length > 0) {
                                waitingToCombineChunkList.add(currentFileChunk.getChunk());
                            }
                        }
                    }
                    if (waitingToCombineChunkList.size() > 0) {
                        combinedFileChunkList.add(combineChunk(waitingToCombineChunkList, currentFileChunk.getUuid(), currentFileChunk.getFileName(), currentFileChunk.getFileType(), startOffset, currentFileChunk.getCombineMode(), lastChunkFlag, originalFileChunkList.get(0).getMeta(), 0, null));
                    } else {
                        if (lastChunkFlag == 1) {
                            combinedFileChunkList.add(currentFileChunk);
                        }
                    }
                    if (duplicateCount > 0) {
                        LOG.error("Duplicate upload chunk, uuid=" + currentFileChunk.getUuid() + ", repetitionCount=" + duplicateCount);
                        duplicateChunkCounter.inc(duplicateCount);
                    }
                }
            } else {
                // 按timestamp排序
                originalFileChunkList.sort(Comparator.comparingLong(FileChunk::getTimestamp));
                long startTimestamp = originalFileChunkList.get(0).getTimestamp();
                StringBuilder timestampAndSizes = new StringBuilder();
                for (FileChunk originalFileChunk : originalFileChunkList) {
                    appendChunkCounter.inc();
                    byte[] chunk = originalFileChunk.getChunk();
                    if (chunk != null && chunk.length > 0) {
                        chunk = originalFileChunk.getChunk();
                        waitingToCombineChunkList.add(chunk);
                        timestampAndSizes.append(originalFileChunk.getTimestamp()).append("-").append(chunk.length).append(";");
                    }
                    if (waitingToCombineChunkList.size() > keyMaxChunk) {
                        break;
                    }
                }
                if (waitingToCombineChunkList.size() > 0) {
                    combinedFileChunkList.add(combineChunk(waitingToCombineChunkList, originalFileChunkList.get(0).getUuid(), originalFileChunkList.get(0).getFileName(), originalFileChunkList.get(0).getFileType(), 0, "append", 0, originalFileChunkList.get(0).getMeta(), startTimestamp, timestampAndSizes.toString()));
                }
            }
        } catch (Exception e) {
            LOG.error("Combiner file error.", e);
            combineErrorCounter.inc();
        }
        return combinedFileChunkList;
    }

    private static FileChunk combineChunk(List<byte[]> byteList, String uuid, String fileName, String fileType, long offset, String combineMode, int lastChunkFlag, Map<String, Object> metaMap, long startTimestamp, String chunkNumbers) {
        FileChunk fileChunk = new FileChunk();
        fileChunk.setChunkCount(byteList.size());
        byte[][] bytes = new byte[byteList.size()][];
        byteList.toArray(bytes);
        byte[] newData = ArrayUtil.addAll(bytes);
        if (COMBINE_MODE_SEEK.equals(combineMode)) {
            fileChunk.setOffset(offset);
            fileChunk.setLastChunkFlag(lastChunkFlag);
        } else {
            if (StringUtil.isNotEmpty(chunkNumbers)) {
                fileChunk.setChunkNumbers(chunkNumbers);
            }
        }
        fileChunk.setTimestamp(startTimestamp);
        fileChunk.setFileType(fileType);
        fileChunk.setUuid(uuid);
        fileChunk.setChunk(newData);
        fileChunk.setFileName(fileName);
        fileChunk.setCombineMode(combineMode);
        fileChunk.setLength(newData.length);
        fileChunk.setMeta(metaMap);
        return fileChunk;
    }

    public static String getUUID() {
        return UUID.randomUUID().toString().replace("-", "").toLowerCase();
    }

    public static String getRowKey(String filename) {
        String md5str = DigestUtil.md5Hex(filename);
        md5str = md5str.substring(8, 24);
        return md5str;
    }

    public static String getIndexDataHead(String filename) {
        return getRowKey(filename).substring(0, 1);
    }
}
