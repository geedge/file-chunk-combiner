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

    public transient Counter completeFilesCounter;
    public transient Counter completeEmlFilesCounter;
    public transient Counter completeTxtFilesCounter;
    public transient Counter startChunksCounter;
    public transient Counter endChunksCounter;
    public transient Counter chunksInCounter;
    public transient Counter chunksOutCounter;
    public transient Counter bytesInCounter;
    public transient Counter bytesOutCounter;
    public transient Counter duplicateChunksCounter;
    public transient Counter errorChunksCounter;
    public transient Counter nullChunksCounter;
    public transient Counter nullTxtChunksCounter;
    public transient Counter nullEmlChunksCounter;
    private final int fileMaxChunkCount;

    public CombineChunkProcessWindowFunction(int fileMaxChunkCount) {
        this.fileMaxChunkCount = fileMaxChunkCount;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("file_chunk_combiner", "window_combine_chunk");
        startChunksCounter = metricGroup.counter("startChunksCount");
        endChunksCounter = metricGroup.counter("endChunksCount");
        duplicateChunksCounter = metricGroup.counter("duplicateChunksCount");
        errorChunksCounter = metricGroup.counter("errorChunksCount");
        chunksInCounter = metricGroup.counter("chunksInCount");
        chunksOutCounter = metricGroup.counter("chunksOutCount");
        bytesInCounter = metricGroup.counter("bytesInCount");
        bytesOutCounter = metricGroup.counter("bytesOutCount");
        completeFilesCounter = metricGroup.counter("completeFilesCount");
        completeEmlFilesCounter = metricGroup.counter("completeEmlFilesCount");
        completeTxtFilesCounter = metricGroup.counter("completeTxtFilesCount");
        metricGroup.meter("numCompleteFilesOutPerSecond", new MeterView(completeFilesCounter));
        metricGroup.meter("numCompleteEmlFilesOutPerSecond", new MeterView(completeEmlFilesCounter));
        metricGroup.meter("numCompleteTxtFilesOutPerSecond", new MeterView(completeTxtFilesCounter));
        metricGroup.meter("numStartChunksOutPerSecond", new MeterView(startChunksCounter));
        metricGroup.meter("numEndChunksOutPerSecond", new MeterView(endChunksCounter));
        metricGroup.meter("numChunksInPerSecond", new MeterView(chunksInCounter));
        metricGroup.meter("numChunksOutPerSecond", new MeterView(chunksOutCounter));
        metricGroup.meter("numBytesInPerSecond", new MeterView(bytesInCounter));
        metricGroup.meter("numBytesOutPerSecond", new MeterView(bytesOutCounter));
        metricGroup.meter("numErrorChunksPerSecond", new MeterView(errorChunksCounter));
        metricGroup.meter("numDuplicateChunksInPerSecond", new MeterView(duplicateChunksCounter));
        nullChunksCounter = metricGroup.counter("nullChunksCount");
        nullEmlChunksCounter = metricGroup.counter("nullTxtChunksCount");
        nullTxtChunksCounter = metricGroup.counter("nullEmlChunksCount");
        metricGroup.meter("numNullFilesOutPerSecond", new MeterView(nullChunksCounter));
        metricGroup.meter("numNullEmlFilesOutPerSecond", new MeterView(nullEmlChunksCounter));
        metricGroup.meter("numNullTxtFilesOutPerSecond", new MeterView(nullTxtChunksCounter));
    }

    @Override
    public void process(String string, Context context, Iterable<FileChunk> elements, Collector<FileChunk> out) {
        List<FileChunk> fileChunks = combine(elements);
        for (FileChunk fileChunk : fileChunks) {
            calculateFileChunkMetrics(fileChunk);
            out.collect(fileChunk);
        }
    }

    private List<FileChunk> combine(Iterable<FileChunk> input) {
        List<FileChunk> combinedFileChunkList = new ArrayList<>();
        List<FileChunk> originalFileChunkList = StreamSupport.stream(input.spliterator(), false).collect(Collectors.toList());
        chunksInCounter.inc(originalFileChunkList.size());
        try {
            List<byte[]> waitingToCombineChunkList = new ArrayList<>();
            if (COMBINE_MODE_SEEK.equals(originalFileChunkList.get(0).getCombineMode())) {
                // 按照offset排序
                originalFileChunkList.sort(Comparator.comparingLong(FileChunk::getOffset));
                Iterator<FileChunk> originalFileChunkIterator = originalFileChunkList.iterator();
                if (originalFileChunkIterator.hasNext()) {
                    int duplicateCount = 0;
                    FileChunk currentFileChunk = originalFileChunkIterator.next();
                    bytesInCounter.inc(currentFileChunk.getLength());
                    int lastChunkFlag = currentFileChunk.getLastChunkFlag();
                    long startOffset = currentFileChunk.getOffset();
                    if (currentFileChunk.getChunk() != null && currentFileChunk.getChunk().length > 0) {
                        waitingToCombineChunkList.add(currentFileChunk.getChunk());
                    }
                    while (originalFileChunkIterator.hasNext()) {
                        long expectedOffset = currentFileChunk.getOffset() + currentFileChunk.getLength();
                        currentFileChunk = originalFileChunkIterator.next();
                        bytesInCounter.inc(currentFileChunk.getLength());
                        long actualOffset = currentFileChunk.getOffset();
                        if (expectedOffset > actualOffset) {// 期望offset大于当前offset，该块为重复块，跳过该块
                            duplicateCount++;
                            duplicateChunksCounter.inc();
                        } else if (expectedOffset == actualOffset) {// 期望offset等于当前offset，将该块添加到待合并集合中
                            if (currentFileChunk.getLastChunkFlag() == 1) {
                                lastChunkFlag = currentFileChunk.getLastChunkFlag();
                            }
                            if (currentFileChunk.getChunk() != null && currentFileChunk.getChunk().length > 0) {
                                waitingToCombineChunkList.add(currentFileChunk.getChunk());
                            }
                        } else {// 期望offset小于当前offset，说明缺块
                            if (!waitingToCombineChunkList.isEmpty()) {//将可合并的chunk合并，清空集合
                                FileChunk fileChunk = combineChunk(waitingToCombineChunkList, currentFileChunk.getUuid(), currentFileChunk.getFileName(), currentFileChunk.getFileType(), startOffset, currentFileChunk.getCombineMode(), lastChunkFlag, originalFileChunkList.get(0).getMeta(), currentFileChunk.getTimestamp(), null);
                                if (fileChunk != null) {
                                    combinedFileChunkList.add(fileChunk);
                                }
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
                    if (!waitingToCombineChunkList.isEmpty()) {
                        FileChunk fileChunk = combineChunk(waitingToCombineChunkList, currentFileChunk.getUuid(), currentFileChunk.getFileName(), currentFileChunk.getFileType(), startOffset, currentFileChunk.getCombineMode(), lastChunkFlag, originalFileChunkList.get(0).getMeta(), currentFileChunk.getTimestamp(), null);
                        if (fileChunk != null) {
                            combinedFileChunkList.add(fileChunk);
                        }
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
                    bytesInCounter.inc(originalFileChunk.getLength());
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
                    FileChunk fileChunk = combineChunk(waitingToCombineChunkList, originalFileChunkList.get(0).getUuid(), originalFileChunkList.get(0).getFileName(), originalFileChunkList.get(0).getFileType(), 0, "append", 0, originalFileChunkList.get(0).getMeta(), startTimestamp, timestampAndSizes.toString());
                    if (fileChunk != null) {
                        combinedFileChunkList.add(fileChunk);
                    }
                }
            }
            chunksOutCounter.inc(combinedFileChunkList.size());
        } catch (Exception e) {
            LOG.error("Combiner chunk error. uuid: " + originalFileChunkList.get(0).getUuid() + ", chunk count: " + originalFileChunkList.size() + ". ", e);
            errorChunksCounter.inc(originalFileChunkList.size());
        }
        return combinedFileChunkList;
    }

    private FileChunk combineChunk(List<byte[]> byteList, String uuid, String fileName, String fileType, long offset, String combineMode, int lastChunkFlag, Map<String, Object> metaMap, long startTimestamp, String chunkNumbers) {
        FileChunk fileChunk = new FileChunk();
        try {
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
            bytesOutCounter.inc(newData.length);
        } catch (Exception e) {
            LOG.error("Combiner chunk error. uuid: " + uuid + ", chunk count: " + byteList.size() + ". ", e);
            errorChunksCounter.inc(byteList.size());
            fileChunk = null;
        }
        return fileChunk;
    }

    private void calculateFileChunkMetrics(FileChunk fileChunk) {
        long offset = fileChunk.getOffset();
        int lastChunkFlag = fileChunk.getLastChunkFlag();
        String fileType = fileChunk.getFileType();
        if (offset == 0 && lastChunkFlag == 1) {
            completeFilesCounter.inc();
            if ("eml".equals(fileType)) {
                completeEmlFilesCounter.inc();
            } else if ("txt".equals(fileType)) {
                completeTxtFilesCounter.inc();
            }
            if (fileChunk.getChunk() == null) {
                nullChunksCounter.inc();
                if ("eml".equals(fileType)) {
                    nullEmlChunksCounter.inc();
                } else if ("txt".equals(fileType)) {
                    nullTxtChunksCounter.inc();
                }
                LOG.info("send file data is null. " + fileChunk.toString());
            }
        } else if (offset == 0) {
            startChunksCounter.inc();
        } else if (lastChunkFlag == 1) {
            endChunksCounter.inc();
        }
    }
}
