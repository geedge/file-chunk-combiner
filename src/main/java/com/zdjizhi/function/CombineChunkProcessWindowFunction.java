package com.zdjizhi.function;

import cn.hutool.core.util.ArrayUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.pojo.FileChunk;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.zdjizhi.utils.PublicConstants.COMBINE_MODE_SEEK;

public class CombineChunkProcessWindowFunction extends ProcessWindowFunction<FileChunk, FileChunk, String, TimeWindow> {
    private static final Log LOG = LogFactory.get();

    public transient Counter duplicateChunkCounter;
    public transient Counter combineErrorCounter;
    private final int fileMaxChunkCount;

    public CombineChunkProcessWindowFunction(int fileMaxChunkCount) {
        this.fileMaxChunkCount = fileMaxChunkCount;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
        duplicateChunkCounter = metricGroup.counter("duplicateChunkCount");
        combineErrorCounter = metricGroup.counter("combineErrorCount");
        metricGroup.meter("duplicateChunkPerSecond", new MeterView(duplicateChunkCounter));
        metricGroup.meter("combineErrorPerSecond", new MeterView(combineErrorCounter));
    }

    @Override
    public void process(String string, Context context, Iterable<FileChunk> elements, Collector<FileChunk> out) {
        List<FileChunk> fileChunks = combine(elements);
        for (FileChunk fileChunk : fileChunks) {
            out.collect(fileChunk);
        }
    }

    private List<FileChunk> combine(Iterable<FileChunk> input) {
        List<FileChunk> combinedFileChunkList = new ArrayList<>();
        try {
            List<FileChunk> originalFileChunkList = StreamSupport.stream(input.spliterator(), false).collect(Collectors.toList());
            List<byte[]> waitingToCombineChunkList = new ArrayList<>();
            if (COMBINE_MODE_SEEK.equals(originalFileChunkList.get(0).getCombineMode())) {
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
                        long expectedOffset = currentFileChunk.getOffset() + currentFileChunk.getLength();
                        currentFileChunk = originalFileChunkIterator.next();
                        long actualOffset = currentFileChunk.getOffset();
                        if (expectedOffset > actualOffset) {// 期望offset大于当前offset，该块为重复块，跳过该块
                            duplicateCount++;
                            duplicateChunkCounter.inc();
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
                    }
                }
            } else {
                // 按timestamp排序
                originalFileChunkList.sort(Comparator.comparingLong(FileChunk::getTimestamp));
                long startTimestamp = originalFileChunkList.get(0).getTimestamp();
                StringBuilder timestampAndSizes = new StringBuilder();
                for (FileChunk originalFileChunk : originalFileChunkList) {
                    byte[] chunk = originalFileChunk.getChunk();
                    if (chunk != null && chunk.length > 0) {
                        chunk = originalFileChunk.getChunk();
                        waitingToCombineChunkList.add(chunk);
                        timestampAndSizes.append(originalFileChunk.getTimestamp()).append("-").append(chunk.length).append(";");
                    }
                    if (waitingToCombineChunkList.size() > fileMaxChunkCount) {
                        break;
                    }
                }
                if (waitingToCombineChunkList.size() > 0) {
                    combinedFileChunkList.add(combineChunk(waitingToCombineChunkList, originalFileChunkList.get(0).getUuid(), originalFileChunkList.get(0).getFileName(), originalFileChunkList.get(0).getFileType(), 0, "append", 0, originalFileChunkList.get(0).getMeta(), startTimestamp, timestampAndSizes.toString()));
                }
            }
        } catch (Exception e) {
            LOG.error("Combiner chunk error.", e);
            combineErrorCounter.inc();
        }
        return combinedFileChunkList;
    }

    private FileChunk combineChunk(List<byte[]> byteList, String uuid, String fileName, String fileType, long offset, String combineMode, int lastChunkFlag, Map<String, Object> metaMap, long startTimestamp, String chunkNumbers) {
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
}
