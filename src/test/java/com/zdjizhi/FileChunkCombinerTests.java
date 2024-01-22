package com.zdjizhi;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.RandomUtil;
import com.zdjizhi.config.Configs;
import com.zdjizhi.function.CombineChunkProcessWindowFunction;
import com.zdjizhi.function.FileChunkKeySelector;
import com.zdjizhi.function.ParseMessagePackMapFunction;
import com.zdjizhi.pojo.FileChunk;
import com.zdjizhi.trigger.LastChunkOrNoDataInTimeTrigger;
import com.zdjizhi.trigger.MultipleTrigger;
import com.zdjizhi.utils.PublicUtil;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.state.JavaSerializer;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableProcessWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.junit.*;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class FileChunkCombinerTests {
    private static Counter duplicateChunkCounter;
    private static Counter combineErrorCounter;
    private static Counter seekChunkCounter;
    private static Counter appendChunkCounter;
    private static Counter sendHosErrorCounter;
    private File emlFile;
    private byte[] emlFileBytes;
    private byte[] pcapngFileBytes;
    private List<FileChunk> inputFileChunks;
    private List<byte[]> messagePackList;
    private List<FileChunk> emlFileChunks;
    private List<FileChunk> pcapngFileChunks;
    private List<FileChunk> pcapngIncludeMetaFileChunks;
    private Map<String, Object> pcapngFileMeta;
    private String emlUuid = "1111111111";
    private String pcapngUuid = "2222222222";
    private String pcapngIncludeMetaUuid = "3333333333";
    private int emlChunkCount = 10;
    private int pcapngChunkCount = 10;
    private long maxChunkCount;
    private String pcapChunkData = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private static Configuration configuration;

    @Before
    public void testBefore() throws Exception {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile("C:\\Users\\root\\Documents\\file-chunk-combiner\\src\\main\\resources\\common.properties");
        configuration = parameterTool.getConfiguration();
        duplicateChunkCounter = new SimpleCounter();
        combineErrorCounter = new SimpleCounter();
        seekChunkCounter = new SimpleCounter();
        appendChunkCounter = new SimpleCounter();
        sendHosErrorCounter = new SimpleCounter();
        maxChunkCount = configuration.get(Configs.COMBINER_WINDOW_KEY_MAX_CHUNK);
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
        inputFileChunks = new ArrayList<>();
        emlFileChunks = new ArrayList<>();
        pcapngFileChunks = new ArrayList<>();
        pcapngIncludeMetaFileChunks = new ArrayList<>();
        ParseMessagePackMapFunction mapFunction = new ParseMessagePackMapFunction();
        ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream("src" + File.separator + "test" + File.separator + "data" + File.separator + "messagePacks"));
        messagePackList = (List<byte[]>) inputStream.readObject();
        for (byte[] messagePack : messagePackList) {
            FileChunk fileChunk = mapFunction.map(messagePack);
            inputFileChunks.add(fileChunk);
        }
    }

    @Test
    public void testParseMessagePack() {
        ParseMessagePackMapFunction mapFunction = new ParseMessagePackMapFunction();
        for (byte[] messagePack : messagePackList) {
            FileChunk fileChunk = mapFunction.map(messagePack);
            Assert.assertNotEquals("解析MessagePack数据失败", null, fileChunk.getUuid());
        }
    }

    @Test
    public void testCombineFullChunk() {
        categorizeChunks(inputFileChunks);
        //测试seek合并模式
        List<FileChunk> fileChunkList = PublicUtil.combine(emlFileChunks, maxChunkCount, duplicateChunkCounter, combineErrorCounter, seekChunkCounter, appendChunkCounter);
        Assert.assertEquals("seek模式合并错误", 1, fileChunkList.size());
        Assert.assertEquals("seek模式合并错误，lastChunkFlag错误", 1, fileChunkList.get(0).getLastChunkFlag());
        Assert.assertEquals("seek模式合并错误，chunkCount错误", emlChunkCount - 1, fileChunkList.get(0).getChunkCount());
        Assert.assertEquals("seek模式合并错误，文件长度错误", emlFileBytes.length, fileChunkList.get(0).getChunk().length);
        Assert.assertEquals("seek模式合并错误，文件内容错误", new String(emlFileBytes), new String(fileChunkList.get(0).getChunk()));
        //测试append合并模式
        fileChunkList = PublicUtil.combine(pcapngFileChunks, maxChunkCount, duplicateChunkCounter, combineErrorCounter, seekChunkCounter, appendChunkCounter);
        Assert.assertEquals("append模式合并错误", 1, fileChunkList.size());
        Assert.assertEquals("append模式合并错误，chunkCount错误", pcapngChunkCount, fileChunkList.get(0).getChunkCount());
        Assert.assertEquals("append模式合并错误，文件长度错误", pcapngFileBytes.length, fileChunkList.get(0).getChunk().length);
        Assert.assertEquals("append模式合并错误，文件内容错误", new String(pcapngFileBytes), new String(fileChunkList.get(0).getChunk()));
        //测试合并携带元信息
        fileChunkList = PublicUtil.combine(pcapngIncludeMetaFileChunks, maxChunkCount, duplicateChunkCounter, combineErrorCounter, seekChunkCounter, appendChunkCounter);
        Assert.assertEquals(1, fileChunkList.size());
        Assert.assertEquals("合并错误，元信息错误", pcapngFileMeta, fileChunkList.get(0).getMeta());

        Assert.assertEquals("监控指标错误", 0, duplicateChunkCounter.getCount());
        Assert.assertEquals("监控指标错误", 0, combineErrorCounter.getCount());
        Assert.assertEquals("监控指标错误", emlChunkCount, seekChunkCounter.getCount());
        Assert.assertEquals("监控指标错误", pcapngChunkCount * 2, appendChunkCounter.getCount());
    }

    @Test
    public void testCombineDuplicateChunk() {
        categorizeChunks(inputFileChunks);
        //测试seek合并模式
        emlFileChunks.add(emlFileChunks.get(5));
        List<FileChunk> fileChunkList = PublicUtil.combine(emlFileChunks, maxChunkCount, duplicateChunkCounter, combineErrorCounter, seekChunkCounter, appendChunkCounter);
        Assert.assertEquals("seek模式合并错误", 1, fileChunkList.size());
        Assert.assertEquals("seek模式合并错误，lastChunkFlag错误", 1, fileChunkList.get(0).getLastChunkFlag());
        Assert.assertEquals("seek模式合并错误，chunkCount错误", emlChunkCount - 1, fileChunkList.get(0).getChunkCount());
        Assert.assertEquals("seek模式合并错误，文件长度错误", emlFileBytes.length, fileChunkList.get(0).getChunk().length);
        Assert.assertEquals("seek模式合并错误，文件内容错误", new String(emlFileBytes), new String(fileChunkList.get(0).getChunk()));
        //测试append合并模式
        pcapngFileChunks.add(pcapngFileChunks.get(5));
        fileChunkList = PublicUtil.combine(pcapngFileChunks, maxChunkCount, duplicateChunkCounter, combineErrorCounter, seekChunkCounter, appendChunkCounter);
        Assert.assertEquals("append模式合并错误", 1, fileChunkList.size());
        Assert.assertEquals("append模式合并错误，chunkCount错误", pcapngChunkCount + 1, fileChunkList.get(0).getChunkCount());
        Assert.assertEquals("append模式合并错误，文件长度错误", pcapngFileBytes.length + pcapChunkData.length(), fileChunkList.get(0).getChunk().length);
        Assert.assertEquals("append模式合并错误，文件内容错误", new String(pcapngFileBytes) + pcapChunkData, new String(fileChunkList.get(0).getChunk()));
        //测试合并携带元信息
        pcapngIncludeMetaFileChunks.add(pcapngIncludeMetaFileChunks.get(5));
        fileChunkList = PublicUtil.combine(pcapngIncludeMetaFileChunks, maxChunkCount, duplicateChunkCounter, combineErrorCounter, seekChunkCounter, appendChunkCounter);
        Assert.assertEquals(1, fileChunkList.size());
        Assert.assertEquals("合并错误，元信息错误", pcapngFileMeta, fileChunkList.get(0).getMeta());

        Assert.assertEquals("监控指标错误", 1, duplicateChunkCounter.getCount());
        Assert.assertEquals("监控指标错误", 0, combineErrorCounter.getCount());
        Assert.assertEquals("监控指标错误", emlChunkCount + 1, seekChunkCounter.getCount());
        Assert.assertEquals("监控指标错误", pcapngChunkCount * 2 + 2, appendChunkCounter.getCount());
    }

    @Test
    public void testCombineLostChunk() {
        categorizeChunks(inputFileChunks);
        //测试seek合并模式
        emlFileChunks.remove(emlFileChunks.get(5));
        List<FileChunk> fileChunkList = PublicUtil.combine(emlFileChunks, maxChunkCount, duplicateChunkCounter, combineErrorCounter, seekChunkCounter, appendChunkCounter);
        Assert.assertEquals("seek模式合并错误", 2, fileChunkList.size());
        Assert.assertEquals("seek模式合并错误，lastChunkFlag错误", 1, fileChunkList.get(1).getLastChunkFlag());
        Assert.assertEquals("append模式合并错误，chunkCount错误", emlChunkCount - 2, fileChunkList.get(0).getChunkCount() + fileChunkList.get(1).getChunkCount());
        Assert.assertEquals("seek模式合并错误，文件长度错误", emlFileBytes.length - 2000, fileChunkList.get(0).getLength() + fileChunkList.get(1).getLength());
        //测试append合并模式
        pcapngFileChunks.remove(pcapngFileChunks.get(5));
        fileChunkList = PublicUtil.combine(pcapngFileChunks, maxChunkCount, duplicateChunkCounter, combineErrorCounter, seekChunkCounter, appendChunkCounter);
        Assert.assertEquals("append模式合并错误", 1, fileChunkList.size());
        Assert.assertEquals("append模式合并错误，chunkCount错误", pcapngChunkCount - 1, fileChunkList.get(0).getChunkCount());
        Assert.assertEquals("append模式合并错误，文件长度错误", pcapngFileBytes.length - pcapChunkData.length(), fileChunkList.get(0).getChunk().length);
        Assert.assertEquals("append模式合并错误，文件内容错误", new String(ArrayUtil.sub(pcapngFileBytes, 0, pcapngFileBytes.length - pcapChunkData.length())), new String(fileChunkList.get(0).getChunk()));
        //测试合并携带元信息
        pcapngIncludeMetaFileChunks.remove(pcapngIncludeMetaFileChunks.get(5));
        fileChunkList = PublicUtil.combine(pcapngIncludeMetaFileChunks, maxChunkCount, duplicateChunkCounter, combineErrorCounter, seekChunkCounter, appendChunkCounter);
        Assert.assertEquals(1, fileChunkList.size());
        Assert.assertEquals("合并错误，元信息错误", pcapngFileMeta, fileChunkList.get(0).getMeta());

        Assert.assertEquals("监控指标错误", 0, duplicateChunkCounter.getCount());
        Assert.assertEquals("监控指标错误", 0, combineErrorCounter.getCount());
        Assert.assertEquals("监控指标错误", emlChunkCount - 1, seekChunkCounter.getCount());
        Assert.assertEquals("监控指标错误", pcapngChunkCount * 2 - 2, appendChunkCounter.getCount());
    }

    @Test
    public void testSendToHos() {
        byte[] data = RandomUtil.randomString(1000).getBytes();
        //seek模式
        FileChunk fileChunk = new FileChunk();
        fileChunk.setUuid("0000000001");
        fileChunk.setCombineMode("seek");
        fileChunk.setFileType("eml");
        fileChunk.setOffset(0);
        fileChunk.setLength(data.length);
        fileChunk.setLastChunkFlag(1);
        fileChunk.setChunkCount(5);
        fileChunk.setTimestamp(System.currentTimeMillis());
        fileChunk.setChunk(data);
        PublicUtil.sendToHos(fileChunk, configuration, sendHosErrorCounter);
        Assert.assertEquals("上传文件到hos错误", 0, sendHosErrorCounter.getCount());
        //append模式
        fileChunk = new FileChunk();
        fileChunk.setUuid("0000000002");
        fileChunk.setCombineMode("append");
        fileChunk.setFileType("pcapng");
        fileChunk.setLength(data.length);
        fileChunk.setChunkCount(5);
        fileChunk.setTimestamp(System.currentTimeMillis());
        fileChunk.setChunk(data);
        fileChunk.setChunkNumbers("1-200,2-200,3-200,4-200,5-200");
        fileChunk.setMeta(pcapngFileMeta);
        PublicUtil.sendToHos(fileChunk, configuration, sendHosErrorCounter);
        Assert.assertEquals("上传文件到hos错误", 0, sendHosErrorCounter.getCount());
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
    public void testCombineChunkProcessWindowFunction() throws Exception {
        ListStateDescriptor listStateDescriptor = new ListStateDescriptor<FileChunk>("test-window", new ListSerializer(new JavaSerializer()));
        WindowOperator<String, FileChunk, FileChunk, FileChunk, TimeWindow> operator = new WindowOperator<String, FileChunk, FileChunk, FileChunk, TimeWindow>(
                TumblingProcessingTimeWindows.of(Time.seconds(3)),
                new TimeWindow.Serializer(),
                new FileChunkKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
                listStateDescriptor,
                new InternalIterableProcessWindowFunction(new CombineChunkProcessWindowFunction(configuration)),
                ProcessingTimeTrigger.create(),
                0L, null);
        KeyedOneInputStreamOperatorTestHarness<String, FileChunk, FileChunk> testHarness = new KeyedOneInputStreamOperatorTestHarness<String, FileChunk, FileChunk>(operator, new FileChunkKeySelector(), BasicTypeInfo.STRING_TYPE_INFO);
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        testHarness.open();
        testHarness.setProcessingTime(3L);
        testHarness.processElement(new StreamRecord<>(inputFileChunks.get(0)));
        testHarness.processElement(new StreamRecord<>(inputFileChunks.get(1)));
        testHarness.processElement(new StreamRecord<>(inputFileChunks.get(2)));
        testHarness.processElement(new StreamRecord<>(inputFileChunks.get(3)));
        testHarness.processElement(new StreamRecord<>(inputFileChunks.get(4)));
        testHarness.setProcessingTime(5000L);
        expectedOutput.add(new StreamRecord<>(PublicUtil.combine(inputFileChunks.subList(0, 5), maxChunkCount, duplicateChunkCounter, combineErrorCounter, seekChunkCounter, appendChunkCounter).get(0), 2999L));
        ConcurrentLinkedQueue<Object> actualOutput = testHarness.getOutput();
        TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, actualOutput, new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
                StreamRecord sr0 = (StreamRecord) o1;
                StreamRecord sr1 = (StreamRecord) o2;
                return Long.compare(((FileChunk) sr0.getValue()).getOffset(), ((FileChunk) sr1.getValue()).getOffset());
            }
        });
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
                .map(new ParseMessagePackMapFunction())
                .filter((FilterFunction<FileChunk>) Objects::nonNull)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(new FileChunkKeySelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowTime)))
                .trigger(trigger)
                .process(new CombineChunkProcessWindowFunction(configuration))
                .addSink(new CollectSink());
        return env;
    }

    private void categorizeChunks(List<FileChunk> fileChunks) {
        for (FileChunk fileChunk : fileChunks) {
            if (emlUuid.equals(fileChunk.getUuid())) {
                emlFileChunks.add(fileChunk);
            } else if (pcapngUuid.equals(fileChunk.getUuid())) {
                pcapngFileChunks.add(fileChunk);
            } else if (pcapngIncludeMetaUuid.equals(fileChunk.getUuid())) {
                pcapngIncludeMetaFileChunks.add(fileChunk);
            }
        }
    }

}
