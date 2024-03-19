package com.zdjizhi;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.RandomUtil;
import com.zdjizhi.config.Configs;
import com.zdjizhi.function.*;
import com.zdjizhi.pojo.FileChunk;
import com.zdjizhi.sink.HBaseSink;
import com.zdjizhi.sink.HosSink;
import com.zdjizhi.trigger.LastChunkOrNoDataInTimeTrigger;
import com.zdjizhi.trigger.MultipleTrigger;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableProcessWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.*;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.zdjizhi.utils.PublicConstants.COMBINE_MODE_APPEND;

public class FileChunkCombinerTests {
    private File emlFile;
    private byte[] emlFileBytes;
    private byte[] pcapngFileBytes;
    private List<FileChunk> inputFileChunks;
    private List<FileChunk> inputFiles;
    private List<byte[]> messagePackList;
    private List<FileChunk> emlFileChunks;
    private List<FileChunk> pcapngFileChunks;
    private List<FileChunk> pcapngIncludeMetaFileChunks;
    private Map<String, Object> pcapngFileMeta;
    private int emlChunkCount = 10;
    private int pcapngChunkCount = 10;
    private String pcapChunkData = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private static Configuration configuration;
    private CombineChunkProcessWindowFunction processWindowFunction;
    private OutputTag<FileChunk> delayedChunkOutputTag;
    private KeyedOneInputStreamOperatorTestHarness<String, FileChunk, FileChunk> testHarness;

    @Before
    public void testBefore() throws Exception {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(FileChunkCombinerTests.class.getClassLoader().getResource("common.properties").getPath());
        configuration = parameterTool.getConfiguration();
        String filePath = "src" + File.separator + "test" + File.separator + "data" + File.separator + "test.eml";
        emlFile = new File(filePath);
        emlFileBytes = FileUtil.readBytes(emlFile);
        StringBuilder pcapData = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            pcapData.append(pcapChunkData);
        }
        pcapngFileBytes = pcapData.toString().getBytes();
        pcapngFileMeta = new HashMap<>();
        pcapngFileMeta.put("ruleId", 151);
        pcapngFileMeta.put("taskId", 7477);
        pcapngFileMeta.put("sledIP", "127.0.0.1");
        emlFileChunks = new ArrayList<>();
        pcapngFileChunks = new ArrayList<>();
        pcapngIncludeMetaFileChunks = new ArrayList<>();
        ObjectInputStream messagePacksInputStream = new ObjectInputStream(new FileInputStream("src" + File.separator + "test" + File.separator + "data" + File.separator + "messagePacks"));
        messagePackList = (List<byte[]>) messagePacksInputStream.readObject();
        messagePacksInputStream.close();
        ObjectInputStream fileChunksInputStream = new ObjectInputStream(new FileInputStream("src" + File.separator + "test" + File.separator + "data" + File.separator + "fileChunks"));
        inputFileChunks = (List<FileChunk>) fileChunksInputStream.readObject();
        fileChunksInputStream.close();
        ObjectInputStream filesInputStream = new ObjectInputStream(new FileInputStream("src" + File.separator + "test" + File.separator + "data" + File.separator + "files"));
        inputFiles = (List<FileChunk>) filesInputStream.readObject();
        filesInputStream.close();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Trigger<Object, TimeWindow>> triggers = new ArrayList<>();
        triggers.add(EventTimeTrigger.create());
        triggers.add(LastChunkOrNoDataInTimeTrigger.of(1000));
        Trigger<Object, TimeWindow> trigger = MultipleTrigger.of(triggers);
        processWindowFunction = new CombineChunkProcessWindowFunction(Integer.MAX_VALUE);
        delayedChunkOutputTag = new OutputTag<FileChunk>("delayed-chunk") {
        };
        DataStreamSource<FileChunk> source = env.fromCollection(inputFileChunks);
        DataStream<FileChunk> window = source
                .keyBy(new FileChunkKeySelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .trigger(trigger)
                .sideOutputLateData(delayedChunkOutputTag)
                .process(processWindowFunction);
        OneInputTransformation<FileChunk, FileChunk> transform = (OneInputTransformation<FileChunk, FileChunk>) window.getTransformation();
        OneInputStreamOperator<FileChunk, FileChunk> operator = transform.getOperator();
        WindowOperator<String, FileChunk, FileChunk, FileChunk, TimeWindow> winOperator = (WindowOperator<String, FileChunk, FileChunk, FileChunk, TimeWindow>) operator;
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO);
    }

    @Test
    public void testParseMessagePackMapFunction() throws Exception {
        ParseMessagePackMapFunction mapFunction = new ParseMessagePackMapFunction(false, Long.MAX_VALUE, "");
        OneInputStreamOperatorTestHarness<byte[], FileChunk> testHarness = new OneInputStreamOperatorTestHarness<>(new StreamMap<>(mapFunction));
        testHarness.setup();
        testHarness.open();
        for (byte[] messagePack : messagePackList) {
            testHarness.processElement(new StreamRecord<>(messagePack));
        }
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        for (FileChunk fileChunk : inputFileChunks) {
            expectedOutput.add(new StreamRecord<>(fileChunk));
        }
        ConcurrentLinkedQueue<Object> actualOutput = testHarness.getOutput();
        Assert.assertEquals(30, actualOutput.size());
        TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, actualOutput, (o1, o2) -> {
            StreamRecord sr0 = (StreamRecord) o1;
            StreamRecord sr1 = (StreamRecord) o2;
            return ((FileChunk) sr0.getValue()).getUuid().compareTo(((FileChunk) sr1.getValue()).getUuid());
        });
        Assert.assertEquals(30, mapFunction.parseMessagePacksCounter.getCount());
        Assert.assertEquals(0, mapFunction.parseErrorMessagePacksCounter.getCount());
        Assert.assertEquals(0, mapFunction.rateLimitDropChunksCounter.getCount());
        Assert.assertEquals(21, mapFunction.equal0BChunksCounter.getCount());
        Assert.assertEquals(1, mapFunction.lessThan1KBChunksCounter.getCount());
        Assert.assertEquals(8, mapFunction.between1KBAnd5KBChunksCounter.getCount());
        Assert.assertEquals(0, mapFunction.between5KBAnd10KBChunksCounter.getCount());
        Assert.assertEquals(0, mapFunction.between10KBAnd50KBChunksCounter.getCount());
        Assert.assertEquals(0, mapFunction.between50KBAnd100KBChunksCounter.getCount());
        Assert.assertEquals(0, mapFunction.greaterThan100KBChunksCounter.getCount());
        Assert.assertEquals(10, mapFunction.emlChunksCounter.getCount());
        Assert.assertEquals(20, mapFunction.pcapngChunksCounter.getCount());
        Assert.assertEquals(0, mapFunction.txtChunksCounter.getCount());
        Assert.assertEquals(0, mapFunction.htmlChunksCounter.getCount());
        Assert.assertEquals(0, mapFunction.mediaChunksCounter.getCount());
        testHarness.close();
    }

    @Test
    public void testSideOutputMapFunction() throws Exception {
        SideOutputMapFunction sideOutputMapFunction = new SideOutputMapFunction();
        OneInputStreamOperatorTestHarness<FileChunk, FileChunk> testHarness = new OneInputStreamOperatorTestHarness<>(new StreamMap<>(sideOutputMapFunction));
        testHarness.setup();
        testHarness.open();
        for (FileChunk fileChunk : inputFileChunks) {
            testHarness.processElement(new StreamRecord<>(fileChunk));
        }
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        for (FileChunk fileChunk : inputFileChunks) {
            fileChunk.setChunkCount(1);
            if (COMBINE_MODE_APPEND.equals(fileChunk.getCombineMode())) {
                fileChunk.setChunkNumbers(fileChunk.getTimestamp() + "-" + fileChunk.getChunk().length + ";");
            }
            expectedOutput.add(new StreamRecord<>(fileChunk));
        }
        ConcurrentLinkedQueue<Object> actualOutput = testHarness.getOutput();
        Assert.assertEquals(30, actualOutput.size());
        TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, actualOutput, (o1, o2) -> {
            StreamRecord sr0 = (StreamRecord) o1;
            StreamRecord sr1 = (StreamRecord) o2;
            return ((FileChunk) sr0.getValue()).getUuid().compareTo(((FileChunk) sr1.getValue()).getUuid());
        });
        Assert.assertEquals(30, sideOutputMapFunction.delayedChunksCounter.getCount());
        testHarness.close();
    }

    @Test
    public void testFileChunkFilterFunction() throws Exception {
        FileChunkFilterFunction fileChunkFilterFunction = new FileChunkFilterFunction(Long.MAX_VALUE, "FileChunk.fileType == \"eml\"");
        StreamFilter<FileChunk> fileChunkStreamFilter = new StreamFilter<>(fileChunkFilterFunction);
        OneInputStreamOperatorTestHarness<FileChunk, FileChunk> testHarness = new OneInputStreamOperatorTestHarness<>(fileChunkStreamFilter);
        testHarness.setup();
        testHarness.open();
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        for (FileChunk fileChunk : inputFileChunks) {
            testHarness.processElement(new StreamRecord<>(fileChunk));
            if ("eml".equals(fileChunk.getFileType())) {
                expectedOutput.add(new StreamRecord<>(fileChunk));
            }
        }
        ConcurrentLinkedQueue<Object> actualOutput = testHarness.getOutput();
        Assert.assertEquals(10, actualOutput.size());
        TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, actualOutput, new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
                StreamRecord sr0 = (StreamRecord) o1;
                StreamRecord sr1 = (StreamRecord) o2;
                return ((FileChunk) sr0.getValue()).getUuid().compareTo(((FileChunk) sr1.getValue()).getUuid());
            }
        });
        Assert.assertEquals(20, fileChunkFilterFunction.filterChunksCounter.getCount());
        testHarness.close();
    }

    @Test
    public void testCombineChunkProcessWindowFunction() throws Exception {
        testHarness.open();
        testHarness.setProcessingTime(0L);
        testHarness.processWatermark(-9223372036854775808L);
        for (FileChunk inputFileChunk : inputFileChunks) {
            testHarness.processElement(new StreamRecord<>(inputFileChunk, inputFileChunk.getTimestamp() / 1000));
        }
        testHarness.setProcessingTime(9223372036854775807L);
        testHarness.processWatermark(9223372036854775807L);
        testHarness.close();
        List<Object> expectedOutput = new ArrayList<>(inputFiles);
        List<Object> actualOutput = new ArrayList<>(testHarness.extractOutputValues());
        Assert.assertEquals(3, actualOutput.size());
        TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, actualOutput, Comparator.comparing(o -> ((FileChunk) o).getUuid()));
        Assert.assertEquals(0, processWindowFunction.combineErrorChunksCounter.getCount());
        Assert.assertEquals(0, processWindowFunction.duplicateChunksCounter.getCount());
        testHarness.close();
    }

    @Test
    public void testCombineChunkProcessWindowFunctionByOutputTag() throws Exception {
        testHarness.open();
        categorizeChunks(inputFileChunks);
        long timestamp = 0L;
        for (FileChunk fileChunk : emlFileChunks) {
            testHarness.processElement(fileChunk, timestamp += 10);
        }
        for (FileChunk fileChunk : pcapngFileChunks) {
            testHarness.processElement(fileChunk, timestamp += 10);
        }
        testHarness.processWatermark(3000L);
        for (FileChunk fileChunk : pcapngIncludeMetaFileChunks) {
            testHarness.processElement(fileChunk, timestamp += 10);
        }
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(inputFiles.get(0));
        expectedOutput.add(inputFiles.get(1));
        List<Object> actualOutput = new ArrayList<>(testHarness.extractOutputValues());
        Assert.assertEquals(2, actualOutput.size());
        TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, actualOutput, Comparator.comparing(o -> ((FileChunk) o).getUuid()));
        ConcurrentLinkedQueue<StreamRecord<FileChunk>> sideOutput = testHarness.getSideOutput(delayedChunkOutputTag);
        List<Object> expectedSideOutput = new ArrayList<>(pcapngIncludeMetaFileChunks);
        List<Object> actualSideOutput = new ArrayList<>();
        for (StreamRecord<FileChunk> streamRecord : sideOutput) {
            actualSideOutput.add(streamRecord.getValue());
        }
        Assert.assertEquals(10, sideOutput.size());
        TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedSideOutput, actualSideOutput, Comparator.comparing(o -> ((FileChunk) o).getUuid()));
        Assert.assertEquals(0, processWindowFunction.combineErrorChunksCounter.getCount());
        Assert.assertEquals(0, processWindowFunction.duplicateChunksCounter.getCount());
        testHarness.close();
    }

    @Test
    public void testCombineChunkProcessWindowFunctionByDuplicateChunk() throws Exception {
        testHarness.open();
        categorizeChunks(inputFileChunks);
        pcapngFileChunks.add(pcapngFileChunks.get(5));
        pcapngIncludeMetaFileChunks.add(pcapngIncludeMetaFileChunks.get(5));
        long timestamp = 0L;
        testHarness.processElement(emlFileChunks.get(5), timestamp + 100);
        for (FileChunk fileChunk : emlFileChunks) {
            testHarness.processElement(fileChunk, timestamp += 10);
        }
        for (FileChunk fileChunk : pcapngFileChunks) {
            testHarness.processElement(fileChunk, timestamp += 10);
        }
        for (FileChunk fileChunk : pcapngIncludeMetaFileChunks) {
            testHarness.processElement(fileChunk, timestamp += 10);
        }
        testHarness.setProcessingTime(5000L);
        List<FileChunk> actualOutput = testHarness.extractOutputValues();
        Assert.assertEquals(3, actualOutput.size());
        Assert.assertEquals(inputFiles.get(0), actualOutput.get(0));
        Assert.assertEquals(inputFiles.get(1).getChunk().length + pcapngFileChunks.get(5).getChunk().length, actualOutput.get(1).getChunk().length);
        Assert.assertEquals(inputFiles.get(2).getChunk().length + pcapngIncludeMetaFileChunks.get(5).getChunk().length, actualOutput.get(2).getChunk().length);
        Assert.assertEquals(0, processWindowFunction.combineErrorChunksCounter.getCount());
        Assert.assertEquals(1, processWindowFunction.duplicateChunksCounter.getCount());
        testHarness.close();
    }

    @Test
    public void testCombineChunkProcessWindowFunctionByLostChunk() throws Exception {
        testHarness.open();
        categorizeChunks(inputFileChunks);
        emlFileChunks.remove(emlFileChunks.get(5));
        pcapngFileChunks.remove(pcapngFileChunks.get(5));
        pcapngIncludeMetaFileChunks.remove(pcapngIncludeMetaFileChunks.get(5));
        long timestamp = 0L;
        for (FileChunk fileChunk : emlFileChunks) {
            testHarness.processElement(fileChunk, timestamp += 10);
        }
        for (FileChunk fileChunk : pcapngFileChunks) {
            testHarness.processElement(fileChunk, timestamp += 10);
        }
        for (FileChunk fileChunk : pcapngIncludeMetaFileChunks) {
            testHarness.processElement(fileChunk, timestamp += 10);
        }
        testHarness.setProcessingTime(5000L);
        List<FileChunk> actualOutput = testHarness.extractOutputValues();
        Assert.assertEquals(4, actualOutput.size());
        Assert.assertEquals(inputFiles.get(0).getChunk().length - emlFileChunks.get(5).getChunk().length, actualOutput.get(0).getChunk().length + actualOutput.get(1).getChunk().length);
        Assert.assertEquals(inputFiles.get(1).getChunk().length - pcapngFileChunks.get(5).getChunk().length, actualOutput.get(2).getChunk().length);
        Assert.assertEquals(inputFiles.get(2).getChunk().length - pcapngIncludeMetaFileChunks.get(5).getChunk().length, actualOutput.get(3).getChunk().length);
        Assert.assertEquals(0, processWindowFunction.combineErrorChunksCounter.getCount());
        Assert.assertEquals(0, processWindowFunction.duplicateChunksCounter.getCount());
        testHarness.close();
    }

    @Test
    public void testHosSink() throws Exception {
        //测试单条上传
        configuration.setString(Configs.SINK_TYPE, "hos");
        configuration.setBoolean(Configs.SINK_BATCH, false);
        HosSink hosSink = new HosSink(configuration);
        StreamSink<FileChunk> fileChunkStreamSink = new StreamSink<>(hosSink);
        OneInputStreamOperatorTestHarness<FileChunk, Object> testHarness = new OneInputStreamOperatorTestHarness<>(fileChunkStreamSink);
        testHarness.setup();
        testHarness.open();
        byte[] data = RandomUtil.randomString(1000).getBytes();
        //seek文件
        FileChunk fileChunk = new FileChunk("0000000001", "eml", 0, data.length, data, "seek", 1, 5, System.currentTimeMillis());
        testHarness.processElement(new StreamRecord<>(fileChunk));
        Assert.assertEquals(1, hosSink.sinkRequestsCounter.getCount());
        Assert.assertEquals(0, hosSink.sinkErrorRequestsCounter.getCount());
        Assert.assertEquals(1, hosSink.sinkFilesCounter.getCount());
        Assert.assertEquals(1, hosSink.sinkChunksCounter.getCount());
        //append文件
        fileChunk = new FileChunk("0000000002", "pcapng", data.length, data, "append", 5, System.currentTimeMillis(), pcapngFileMeta, "1-200,2-200,3-200,4-200,5-200");
        testHarness.processElement(new StreamRecord<>(fileChunk));
        Assert.assertEquals(2, hosSink.sinkRequestsCounter.getCount());
        Assert.assertEquals(0, hosSink.sinkErrorRequestsCounter.getCount());
        Assert.assertEquals(1, hosSink.sinkFilesCounter.getCount());
        Assert.assertEquals(2, hosSink.sinkChunksCounter.getCount());
        Assert.assertEquals(2, hosSink.sinkChunksCounter.getCount());
        Assert.assertEquals(2, hosSink.sinkChunksCounter.getCount());
        Assert.assertEquals(2, hosSink.sinkChunksCounter.getCount());
        Assert.assertEquals(2, hosSink.lessThan5KBChunksCounter.getCount());
        Assert.assertEquals(0, hosSink.between5KBAnd10KBChunksCounter.getCount());
        Assert.assertEquals(0, hosSink.between10KBAnd50KBChunksCounter.getCount());
        Assert.assertEquals(0, hosSink.between50KBAnd100KBChunksCounter.getCount());
        Assert.assertEquals(0, hosSink.between100KBAnd1MBChunksCounter.getCount());
        Assert.assertEquals(0, hosSink.greaterThan1MBChunksCounter.getCount());

        testHarness.close();
        //测试批量上传
        data = RandomUtil.randomString(10000).getBytes();
        configuration.setString(Configs.SINK_TYPE, "hos");
        configuration.setBoolean(Configs.SINK_BATCH, true);
        configuration.setInteger(Configs.SINK_BATCH_COUNT, 2);
        hosSink = new HosSink(configuration);
        fileChunkStreamSink = new StreamSink<>(hosSink);
        testHarness = new OneInputStreamOperatorTestHarness<>(fileChunkStreamSink);
        testHarness.setup();
        testHarness.open();
        fileChunk = new FileChunk("0000000001", "eml", 0, data.length, data, "seek", 1, 5, System.currentTimeMillis());
        testHarness.processElement(new StreamRecord<>(fileChunk));
        fileChunk = new FileChunk("0000000002", "eml", 0, data.length, data, "seek", 1, 5, System.currentTimeMillis());
        testHarness.processElement(new StreamRecord<>(fileChunk));
        Assert.assertEquals(1, hosSink.sinkRequestsCounter.getCount());
        Assert.assertEquals(0, hosSink.sinkErrorRequestsCounter.getCount());
        Assert.assertEquals(2, hosSink.sinkFilesCounter.getCount());
        Assert.assertEquals(2, hosSink.sinkChunksCounter.getCount());
        Assert.assertEquals(0, hosSink.lessThan5KBChunksCounter.getCount());
        Assert.assertEquals(2, hosSink.between5KBAnd10KBChunksCounter.getCount());
        Assert.assertEquals(0, hosSink.between10KBAnd50KBChunksCounter.getCount());
        Assert.assertEquals(0, hosSink.between50KBAnd100KBChunksCounter.getCount());
        Assert.assertEquals(0, hosSink.between100KBAnd1MBChunksCounter.getCount());
        Assert.assertEquals(0, hosSink.greaterThan1MBChunksCounter.getCount());
        testHarness.close();
    }

    @Test
    public void testHBaseSink() throws Exception {
        configuration.setString(Configs.SINK_TYPE, "hbase");
        configuration.setBoolean(Configs.SINK_BATCH, true);
        configuration.setInteger(Configs.SINK_BATCH_COUNT, 2);
        HBaseSink hBaseSink = new HBaseSink(configuration);
        StreamSink<FileChunk> fileChunkStreamSink = new StreamSink<>(hBaseSink);
        OneInputStreamOperatorTestHarness<FileChunk, Object> testHarness = new OneInputStreamOperatorTestHarness<>(fileChunkStreamSink);
        testHarness.setup();
        testHarness.open();
        byte[] data = RandomUtil.randomString(1000).getBytes();
        FileChunk fileChunk = new FileChunk("0000000001", "eml", 0, data.length, data, "seek", 1, 5, System.currentTimeMillis());
        testHarness.processElement(new StreamRecord<>(fileChunk));
        fileChunk = new FileChunk("0000000002", "eml", 0, data.length, data, "seek", 1, 5, System.currentTimeMillis());
        testHarness.processElement(new StreamRecord<>(fileChunk));
        Assert.assertEquals(3, hBaseSink.sinkRequestsCounter.getCount());
        Assert.assertEquals(0, hBaseSink.sinkErrorRequestsCounter.getCount());
        Assert.assertEquals(2, hBaseSink.sinkFilesCounter.getCount());
        Assert.assertEquals(2, hBaseSink.sinkChunksCounter.getCount());
        Assert.assertEquals(2, hBaseSink.lessThan5KBChunksCounter.getCount());
        Assert.assertEquals(0, hBaseSink.between5KBAnd10KBChunksCounter.getCount());
        Assert.assertEquals(0, hBaseSink.between10KBAnd50KBChunksCounter.getCount());
        Assert.assertEquals(0, hBaseSink.between50KBAnd100KBChunksCounter.getCount());
        Assert.assertEquals(0, hBaseSink.between100KBAnd1MBChunksCounter.getCount());
        Assert.assertEquals(0, hBaseSink.greaterThan1MBChunksCounter.getCount());
        testHarness.close();
    }

    @Test
    public void testPipelineFullChunk() throws Exception {
        CollectSink.values.clear();
        long windowTime = 5;
        messagePackList.sort(Comparator.comparingInt(Arrays::hashCode));//打乱顺序
        StreamExecutionEnvironment env = createPipeline(2, new ByteDataSource(messagePackList, 500, windowTime), windowTime, 1);
        env.execute();
        List<FileChunk> fileChunks = CollectSink.values;
        Assert.assertTrue("合并错误,sink输出错误", fileChunks.size() > 0);
        categorizeChunks(fileChunks);
        byte[] data = new byte[0];
        long length = 0;
        long chunkCount = 0;
        int lastChunkFlag = 0;
        emlFileChunks.sort(Comparator.comparingLong(FileChunk::getOffset));
        for (FileChunk fileChunk : emlFileChunks) {
            data = ArrayUtil.addAll(data, fileChunk.getChunk());
            length += fileChunk.getLength();
            chunkCount += fileChunk.getChunkCount();
            if (fileChunk.getLastChunkFlag() == 1) {
                lastChunkFlag = 1;
            }
        }
        Assert.assertEquals("seek模式合并错误，lastChunkFlag错误", 1, lastChunkFlag);
        Assert.assertEquals("seek模式合并错误，chunkCount错误", emlChunkCount - 1, chunkCount);
        Assert.assertEquals("seek模式合并错误，文件长度错误", emlFile.length(), length);
        Assert.assertEquals("seek模式合并错误，文件内容错误", new String(emlFileBytes), new String(data));
        data = new byte[0];
        length = 0;
        chunkCount = 0;
        for (FileChunk fileChunk : pcapngFileChunks) {
            data = ArrayUtil.addAll(data, fileChunk.getChunk());
            length += fileChunk.getLength();
            chunkCount += fileChunk.getChunkCount();
        }
        Assert.assertEquals("append模式合并错误，chunkCount错误", pcapngChunkCount, chunkCount);
        Assert.assertEquals("append模式合并错误，文件长度错误", pcapngFileBytes.length, length);
        Assert.assertEquals("append模式合并错误，文件内容错误", new String(pcapngFileBytes), new String(data));
        data = new byte[0];
        length = 0;
        chunkCount = 0;
        for (FileChunk fileChunk : pcapngIncludeMetaFileChunks) {
            data = ArrayUtil.addAll(data, fileChunk.getChunk());
            length += fileChunk.getLength();
            chunkCount += fileChunk.getChunkCount();
            Assert.assertEquals("合并错误，元信息错误", pcapngFileMeta, fileChunk.getMeta());
        }
    }

    @Test
    public void testPipelineLostChunk() throws Exception {
        CollectSink.values.clear();
        long windowTime = 5;
        //删除部分chunk
        messagePackList.remove(5);
        messagePackList.remove(15);
        messagePackList.remove(25);
        messagePackList.sort(Comparator.comparingInt(Arrays::hashCode));//打乱顺序
        StreamExecutionEnvironment env = createPipeline(2, new ByteDataSource(messagePackList, 500, windowTime), windowTime, 1);
        env.execute();
        List<FileChunk> fileChunks = CollectSink.values;
        Assert.assertTrue("合并错误,sink输出错误", fileChunks.size() > 0);
        categorizeChunks(fileChunks);
        byte[] data = new byte[0];
        long length = 0;
        long chunkCount = 0;
        int lastChunkFlag = 0;
        emlFileChunks.sort(Comparator.comparingLong(FileChunk::getOffset));
        for (FileChunk fileChunk : emlFileChunks) {
            data = ArrayUtil.addAll(data, fileChunk.getChunk());
            length += fileChunk.getLength();
            chunkCount += fileChunk.getChunkCount();
            if (fileChunk.getLastChunkFlag() == 1) {
                lastChunkFlag = 1;
            }
        }
        Assert.assertEquals("seek模式合并错误，lastChunkFlag错误", 1, lastChunkFlag);
        Assert.assertEquals("seek模式合并错误，chunkCount错误", emlChunkCount - 2, chunkCount);
        Assert.assertEquals("seek模式合并错误，文件长度错误", emlFileBytes.length - 2000, length);
        data = new byte[0];
        length = 0;
        chunkCount = 0;
        for (FileChunk fileChunk : pcapngFileChunks) {
            data = ArrayUtil.addAll(data, fileChunk.getChunk());
            length += fileChunk.getLength();
            chunkCount += fileChunk.getChunkCount();
        }
        Assert.assertEquals("append模式合并错误，chunkCount错误", pcapngChunkCount - 1, chunkCount);
        Assert.assertEquals("append模式合并错误，文件长度错误", pcapngFileBytes.length - pcapChunkData.length(), length);
        Assert.assertEquals("append模式合并错误，文件内容错误", new String(ArrayUtil.sub(pcapngFileBytes, 0, pcapngFileBytes.length - pcapChunkData.length())), new String(data));
        data = new byte[0];
        length = 0;
        chunkCount = 0;
        for (FileChunk fileChunk : pcapngIncludeMetaFileChunks) {
            data = ArrayUtil.addAll(data, fileChunk.getChunk());
            length += fileChunk.getLength();
            chunkCount += fileChunk.getChunkCount();
            Assert.assertEquals("合并错误，元信息错误", pcapngFileMeta, fileChunk.getMeta());
        }
    }

    @Test
    public void testPipelineDuplicateChunk() throws Exception {
        CollectSink.values.clear();
        long windowTime = 5;
        //添加重复chunk
        messagePackList.add(messagePackList.get(5));
        messagePackList.add(messagePackList.get(15));
        messagePackList.add(messagePackList.get(25));
        messagePackList.sort(Comparator.comparingInt(Arrays::hashCode));//打乱顺序
        StreamExecutionEnvironment env = createPipeline(2, new ByteDataSource(messagePackList, 500, windowTime), windowTime, 1);
        env.execute();
        List<FileChunk> fileChunks = CollectSink.values;
        Assert.assertTrue("合并错误,sink输出错误", fileChunks.size() > 0);
        categorizeChunks(fileChunks);
        byte[] data = new byte[0];
        long length = 0;
        long chunkCount = 0;
        int lastChunkFlag = 0;
        emlFileChunks.sort(Comparator.comparingLong(FileChunk::getOffset));
        for (FileChunk fileChunk : emlFileChunks) {
            data = ArrayUtil.addAll(data, fileChunk.getChunk());
            length += fileChunk.getLength();
            chunkCount += fileChunk.getChunkCount();
            if (fileChunk.getLastChunkFlag() == 1) {
                lastChunkFlag = 1;
            }
        }
        Assert.assertEquals("seek模式合并错误，lastChunkFlag错误", 1, lastChunkFlag);
        Assert.assertEquals("seek模式合并错误，chunkCount错误", emlChunkCount - 1, chunkCount);
        Assert.assertEquals("seek模式合并错误，文件长度错误", emlFileBytes.length, length);
        data = new byte[0];
        length = 0;
        chunkCount = 0;
        for (FileChunk fileChunk : pcapngFileChunks) {
            data = ArrayUtil.addAll(data, fileChunk.getChunk());
            length += fileChunk.getLength();
            chunkCount += fileChunk.getChunkCount();
        }
        Assert.assertEquals("append模式合并错误，chunkCount错误", pcapngChunkCount + 1, chunkCount);
        Assert.assertEquals("append模式合并错误，文件长度错误", pcapngFileBytes.length + pcapChunkData.length(), length);
        Assert.assertEquals("append模式合并错误，文件内容错误", new String(ArrayUtil.addAll(pcapngFileBytes, ArrayUtil.sub(pcapngFileBytes, 0, pcapChunkData.length()))), new String(data));
        data = new byte[0];
        length = 0;
        chunkCount = 0;
        for (FileChunk fileChunk : pcapngIncludeMetaFileChunks) {
            data = ArrayUtil.addAll(data, fileChunk.getChunk());
            length += fileChunk.getLength();
            chunkCount += fileChunk.getChunkCount();
            Assert.assertEquals("合并错误，元信息错误", pcapngFileMeta, fileChunk.getMeta());
        }
    }

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(2)
                            .build());

    private static class CollectSink implements SinkFunction<FileChunk> {
        private static final List<FileChunk> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(FileChunk value, Context context) {
            values.add(value);
        }
    }

    private static class ByteDataSource implements SourceFunction<byte[]> {
        private volatile boolean isRunning = true;
        private final List<byte[]> dataList;
        private final long delay;
        private final long windowTime;

        ByteDataSource(List<byte[]> dataList, long delay, long windowTime) {
            this.dataList = dataList;
            this.delay = delay;
            this.windowTime = windowTime;
        }

        @Override
        public void run(SourceContext<byte[]> ctx) throws Exception {
            int index = 0;
            while (isRunning && index < dataList.size()) {
                byte[] record = dataList.get(index);
                ctx.collect(record);
                index++;
                Thread.sleep(delay);
            }
            // 发送完数据后，等待窗口执行完成
            Thread.sleep(windowTime * 1000);
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    @Test
    public void testMock() throws Exception {
        ProcessWindowFunctionMock mock = Mockito.mock(ProcessWindowFunctionMock.class);
        InternalIterableProcessWindowFunction<FileChunk, FileChunk, String, TimeWindow> windowFunction = new InternalIterableProcessWindowFunction<>(mock);
        TypeInformation<FileChunk> fileChunkType = PojoTypeInfo.of(FileChunk.class);
        ExecutionConfig execConf = new ExecutionConfig();
        execConf.setParallelism(5);
        StreamingFunctionUtils.setOutputType(windowFunction, fileChunkType, execConf);
        Mockito.verify(mock).setOutputType(fileChunkType, execConf);
        Configuration config = new Configuration();
        windowFunction.open(config);
        Mockito.verify(mock).open(config);
        RuntimeContext rCtx = Mockito.mock(RuntimeContext.class);
        windowFunction.setRuntimeContext(rCtx);
        (Mockito.verify(mock)).setRuntimeContext(rCtx);
        TimeWindow w = Mockito.mock(TimeWindow.class);
        Iterable<FileChunk> i = Mockito.mock(Iterable.class);
        Collector<FileChunk> c = Mockito.mock(Collector.class);
        InternalWindowFunction.InternalWindowContext ctx = Mockito.mock(InternalWindowFunction.InternalWindowContext.class);
        (Mockito.doAnswer(new Answer() {
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ProcessWindowFunction<FileChunk, FileChunk, String, TimeWindow>.Context c = (ProcessWindowFunction.Context) invocationOnMock.getArguments()[1];
                c.currentProcessingTime();
                c.currentWatermark();
                c.windowState();
                c.globalState();
                return null;
            }
        }).when(mock)).process(Mockito.anyString(), Mockito.anyObject(), Mockito.eq(i), Mockito.eq(c));
        windowFunction.process("", w, ctx, i, c);
        Mockito.verify(ctx).currentProcessingTime();
        Mockito.verify(ctx).currentWatermark();
        Mockito.verify(ctx).windowState();
        Mockito.verify(ctx).globalState();
        windowFunction.close();
        Mockito.verify(mock).close();
    }

    private static class ProcessWindowFunctionMock extends ProcessWindowFunction<FileChunk, FileChunk, String, TimeWindow> implements OutputTypeConfigurable<FileChunk> {
        private static final long serialVersionUID = 1L;

        private ProcessWindowFunctionMock() {
        }

        @Override
        public void process(String s, Context context, Iterable<FileChunk> elements, Collector<FileChunk> out) throws Exception {
        }

        public void setOutputType(TypeInformation<FileChunk> outTypeInfo, ExecutionConfig executionConfig) {
        }
    }

    private StreamExecutionEnvironment createPipeline(int parallelism, SourceFunction<byte[]> source, long windowTime, long windowIdleTime) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        WatermarkStrategy<FileChunk> watermarkStrategy = WatermarkStrategy
                .<FileChunk>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((FileChunk, timestamp) -> FileChunk.getTimestamp() / 1000);
        List<Trigger<Object, TimeWindow>> triggers = new ArrayList<>();
        triggers.add(EventTimeTrigger.create());
        triggers.add(LastChunkOrNoDataInTimeTrigger.of(windowIdleTime * 1000));
        Trigger<Object, TimeWindow> trigger = MultipleTrigger.of(triggers);
        env.addSource(source)
                .map(new ParseMessagePackMapFunction(false, Long.MAX_VALUE, ""))
                .filter(new FileChunkFilterFunction(Long.MAX_VALUE, ""))
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(new FileChunkKeySelector(), BasicTypeInfo.STRING_TYPE_INFO)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowTime)))
                .trigger(trigger)
                .process(new CombineChunkProcessWindowFunction(Integer.MAX_VALUE))
                .addSink(new CollectSink());
        return env;
    }

    private void categorizeChunks(List<FileChunk> fileChunks) {
        for (FileChunk fileChunk : fileChunks) {
            if ("eml".equals(fileChunk.getFileType())) {
                emlFileChunks.add(fileChunk);
            } else if ("pcapng".equals(fileChunk.getFileType()) && fileChunk.getMeta() == null) {
                pcapngFileChunks.add(fileChunk);
            } else if ("pcapng".equals(fileChunk.getFileType()) && fileChunk.getMeta() != null) {
                pcapngIncludeMetaFileChunks.add(fileChunk);
            }
        }
    }

//    @Test
//    public void testCombineChunkProcessWindowFunction() throws Exception {
//        List<Trigger<Object, TimeWindow>> triggers = new ArrayList<>();
//        triggers.add(EventTimeTrigger.create());
//        triggers.add(LastChunkOrNoDataInTimeTrigger.of(1000));
//        Trigger<Object, TimeWindow> trigger = MultipleTrigger.of(triggers);
//        TypeSerializer<FileChunk> serializer = TypeInformation.of(FileChunk.class).createSerializer(new ExecutionConfig());
//        ListStateDescriptor listStateDescriptor = new ListStateDescriptor<>("test-seek-window", serializer);
//        CombineChunkProcessWindowFunction processWindowFunction = new CombineChunkProcessWindowFunction(Integer.MAX_VALUE);
//        WindowOperator<String, FileChunk, FileChunk, FileChunk, TimeWindow> operator = new WindowOperator<String, FileChunk, FileChunk, FileChunk, TimeWindow>(
//                TumblingEventTimeWindows.of(Time.seconds(3)),
//                new TimeWindow.Serializer(),
//                new FileChunkKeySelector(),
//                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
//                listStateDescriptor,
//                new InternalIterableProcessWindowFunction(processWindowFunction),
//                trigger,
//                0L, null);
//        KeyedOneInputStreamOperatorTestHarness<String, FileChunk, FileChunk> testHarness = new KeyedOneInputStreamOperatorTestHarness<>(operator, new FileChunkKeySelector(), BasicTypeInfo.STRING_TYPE_INFO);
//        testHarness.setup();
//        testHarness.open();
//        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
//        for (FileChunk file : inputFiles) {
//            expectedOutput.add(new StreamRecord<>(file, 2999L));
//        }
//        long timestamp = 0L;
//        for (FileChunk fileChunk : inputFileChunks) {
//            testHarness.processElement(fileChunk, timestamp += 10);
//        }
//        testHarness.setProcessingTime(5000L);
//        ConcurrentLinkedQueue<Object> actualOutput = testHarness.getOutput();
//        Assert.assertEquals(3, actualOutput.size());
//        TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, actualOutput, (o1, o2) -> {
//            StreamRecord sr0 = (StreamRecord) o1;
//            StreamRecord sr1 = (StreamRecord) o2;
//            return ((FileChunk) sr0.getValue()).getUuid().compareTo(((FileChunk) sr1.getValue()).getUuid());
//        });
//        Assert.assertEquals(0, processWindowFunction.combineErrorCounter.getCount());
//        Assert.assertEquals(0, processWindowFunction.duplicateChunkCounter.getCount());
//        testHarness.close();
//    }
}
