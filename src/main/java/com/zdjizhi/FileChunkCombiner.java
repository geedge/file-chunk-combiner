package com.zdjizhi;

import cn.hutool.core.util.StrUtil;
import com.zdjizhi.config.Configs;
import com.zdjizhi.function.*;
import com.zdjizhi.function.map.ParseMessagePackMapFunction;
import com.zdjizhi.function.map.ParseProxyFileMetaFlatMapFunction;
import com.zdjizhi.function.map.ParseSessionFileMetaFlatMapFunction;
import com.zdjizhi.function.map.SideOutputMapFunction;
import com.zdjizhi.kafka.FileMetaKafkaConsumer;
import com.zdjizhi.pojo.*;
import com.zdjizhi.sink.*;
import com.zdjizhi.kafka.KafkaConsumer;
import com.zdjizhi.trigger.IdleTimeTrigger;
import com.zdjizhi.trigger.LastChunkOrNoDataInTimeTrigger;
import com.zdjizhi.trigger.LastChunkTrigger;
import com.zdjizhi.trigger.MultipleTrigger;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.*;

public class FileChunkCombiner {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);
        final Configuration configuration = parameterTool.getConfiguration();
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().setGlobalJobParameters(configuration);

        SingleOutputStreamOperator<FileChunk> windowStream;
        OutputTag<FileChunk> delayedChunkOutputTag = new OutputTag<FileChunk>("delayed-chunk") {
        };
        if (configuration.getInteger(Configs.COMBINER_WINDOW_TYPE) == 0) {
            WatermarkStrategy<FileChunk> watermarkStrategy = WatermarkStrategy
                    .<FileChunk>forBoundedOutOfOrderness(Duration.ofSeconds(configuration.get(Configs.COMBINER_WINDOW_ALLOWED_LATENESS)))
                    .withTimestampAssigner((FileChunk, timestamp) -> FileChunk.getTimestamp() / 1000);

            SingleOutputStreamOperator<FileChunk> parseMessagePackStream = environment
                    .addSource(KafkaConsumer.byteArrayConsumer(configuration))
                    .name(configuration.get(Configs.KAFKA_TOPIC))
                    .map(new ParseMessagePackMapFunction())
                    .name("Map: Parse Message Pack")
                    .filter(new FileChunkFilterFunction(configuration.getString(Configs.MAP_FILTER_EXPRESSION), "map_parse_message_pack"))
                    .name("Filter: Map")
                    .assignTimestampsAndWatermarks(watermarkStrategy);

            List<Trigger<Object, TimeWindow>> triggers = new ArrayList<>();
            triggers.add(EventTimeTrigger.create());
            if (configuration.get(Configs.COMBINER_WINDOW_ENABLE_LAST_CHUNK_TRIGGER)) {
                triggers.add(LastChunkOrNoDataInTimeTrigger.of(configuration.get(Configs.COMBINER_WINDOW_IDLE_TIME) * 1000));
            } else {
                triggers.add(IdleTimeTrigger.of(configuration.get(Configs.COMBINER_WINDOW_IDLE_TIME) * 1000));
            }
            Trigger<Object, TimeWindow> trigger = MultipleTrigger.of(triggers);
            windowStream = parseMessagePackStream
                    .keyBy(new FileChunkKeySelector(), BasicTypeInfo.STRING_TYPE_INFO)
                    .window(TumblingEventTimeWindows.of(Time.seconds(configuration.get(Configs.COMBINER_WINDOW_TIME))))
                    .trigger(trigger)
                    .sideOutputLateData(delayedChunkOutputTag)
                    .allowedLateness(Time.seconds(configuration.get(Configs.COMBINER_WINDOW_ALLOWED_LATENESS)))
                    .process(new CombineChunkProcessWindowFunction(configuration.get(Configs.FILE_MAX_CHUNK_COUNT)))
                    .name("Window: Combine Chunk")
                    .setParallelism(configuration.get(Configs.COMBINER_WINDOW_PARALLELISM));
        } else {
            SingleOutputStreamOperator<FileChunk> parseMessagePackStream = environment
                    .addSource(KafkaConsumer.byteArrayConsumer(configuration))
                    .name(configuration.get(Configs.KAFKA_TOPIC))
                    .map(new ParseMessagePackMapFunction())
                    .name("Map: Parse Message Pack")
                    .filter(new FileChunkFilterFunction(configuration.getString(Configs.MAP_FILTER_EXPRESSION), "map_parse_message_pack"))
                    .name("Filter: Map");

            List<Trigger<Object, TimeWindow>> triggers = new ArrayList<>();
            triggers.add(ProcessingTimeTrigger.create());
            if (configuration.get(Configs.COMBINER_WINDOW_ENABLE_LAST_CHUNK_TRIGGER)) {
                triggers.add(LastChunkTrigger.create());
            }
            Trigger<Object, TimeWindow> trigger = MultipleTrigger.of(triggers);
            windowStream = parseMessagePackStream
                    .keyBy(new FileChunkKeySelector(), BasicTypeInfo.STRING_TYPE_INFO)
                    .window(TumblingProcessingTimeWindows.of(Time.seconds(configuration.get(Configs.COMBINER_WINDOW_TIME))))
                    .trigger(trigger)
                    .process(new CombineChunkProcessWindowFunction(configuration.get(Configs.FILE_MAX_CHUNK_COUNT)))
                    .name("Window: Combine Chunk")
                    .setParallelism(configuration.get(Configs.COMBINER_WINDOW_PARALLELISM));
        }

        for (String sinkType : configuration.get(Configs.SINK_TYPE).split(",")) {
            switch (sinkType) {
                case "hos":
                    DataStream<FileChunk> sideOutput = windowStream.getSideOutput(delayedChunkOutputTag);
                    if (StrUtil.isNotEmpty(configuration.getString(Configs.SINK_FILTER_EXPRESSION))) {
                        windowStream
                                .filter(new FileChunkFilterFunction(configuration.getString(Configs.SINK_FILTER_EXPRESSION), "sink_hos"))
                                .name("Filter: Hos")
                                .setParallelism(configuration.get(Configs.SINK_PARALLELISM))
                                .addSink(new HosSink(configuration))
                                .name("Hos")
                                .setParallelism(configuration.get(Configs.SINK_PARALLELISM));
                        sideOutput = sideOutput
                                .filter(new FileChunkFilterFunction(configuration.getString(Configs.SINK_FILTER_EXPRESSION), "side_out_put_hos"))
                                .name("Filter: Delayed Chunk")
                                .setParallelism(configuration.get(Configs.SINK_PARALLELISM));
                    } else {
                        windowStream
                                .addSink(new HosSink(configuration))
                                .name("Hos")
                                .setParallelism(configuration.get(Configs.SINK_PARALLELISM));
                    }
                    sideOutput.map(new SideOutputMapFunction())
                            .setParallelism(configuration.get(Configs.SINK_PARALLELISM))
                            .addSink(new HosSink(configuration))
                            .setParallelism(configuration.get(Configs.SINK_PARALLELISM))
                            .name("Delayed Chunk");
                    break;
                case "hbase":
                    sideOutput = windowStream.getSideOutput(delayedChunkOutputTag);
                    if (StrUtil.isNotEmpty(configuration.getString(Configs.SINK_FILTER_EXPRESSION))) {
                        windowStream
                                .filter(new FileChunkFilterFunction(configuration.getString(Configs.SINK_FILTER_EXPRESSION), "sink_hbase"))
                                .name("Filter: HBase")
                                .setParallelism(configuration.get(Configs.SINK_PARALLELISM))
                                .addSink(new HBaseSink(configuration))
                                .name("HBase")
                                .setParallelism(configuration.get(Configs.SINK_PARALLELISM));
                        sideOutput = sideOutput
                                .filter(new FileChunkFilterFunction(configuration.getString(Configs.SINK_FILTER_EXPRESSION), "side_out_put_hbase"))
                                .name("Filter: Delayed Chunk")
                                .setParallelism(configuration.get(Configs.SINK_PARALLELISM));
                    } else {
                        windowStream
                                .addSink(new HBaseSink(configuration))
                                .name("HBase")
                                .setParallelism(configuration.get(Configs.SINK_PARALLELISM));
                    }
                    sideOutput
                            .map(new SideOutputMapFunction())
                            .setParallelism(configuration.get(Configs.SINK_PARALLELISM))
                            .addSink(new HBaseSink(configuration))
                            .setParallelism(configuration.get(Configs.SINK_PARALLELISM))
                            .name("Delayed Chunk");
                    break;
                case "oss":
                    SingleOutputStreamOperator<FileChunk> fileMetaSessionSingleOutputStreamOperator = environment.addSource(FileMetaKafkaConsumer.stringConsumer(configuration, configuration.get(Configs.KAFKA_FILE_META_SESSION_TOPIC)))
                            .setParallelism(configuration.get(Configs.MAP_PARSE_FILE_META_PARALLELISM))
                            .name(configuration.get(Configs.KAFKA_FILE_META_SESSION_TOPIC))
                            .flatMap(new ParseSessionFileMetaFlatMapFunction())
                            .setParallelism(configuration.get(Configs.MAP_PARSE_FILE_META_PARALLELISM))
                            .name("Map: Parse Session File Meta")
                            .filter(new FileChunkFilterFunction(configuration.getString(Configs.FILE_META_FILTER_EXPRESSION), "map_parse_session_file_meta"))
                            .name("Filter: Map")
                            .setParallelism(configuration.get(Configs.MAP_PARSE_FILE_META_PARALLELISM));
                    SingleOutputStreamOperator<FileChunk> fileMetaProxySingleOutputStreamOperator = environment.addSource(FileMetaKafkaConsumer.stringConsumer(configuration, configuration.get(Configs.KAFKA_FILE_META_PROXY_TOPIC)))
                            .setParallelism(configuration.get(Configs.MAP_PARSE_FILE_META_PARALLELISM))
                            .name(configuration.get(Configs.KAFKA_FILE_META_PROXY_TOPIC))
                            .flatMap(new ParseProxyFileMetaFlatMapFunction())
                            .setParallelism(configuration.get(Configs.MAP_PARSE_FILE_META_PARALLELISM))
                            .name("Map: Parse Proxy File Meta")
                            .filter(new FileChunkFilterFunction(configuration.getString(Configs.FILE_META_FILTER_EXPRESSION), "map_parse_proxy_file_meta"))
                            .name("Filter: Map")
                            .setParallelism(configuration.get(Configs.MAP_PARSE_FILE_META_PARALLELISM));
                    windowStream
                            .filter(new FileChunkFilterFunction(configuration.getString(Configs.SINK_OSS_FILTER_EXPRESSION), "sink_oss"))
                            .name("Filter: Oss")
                            .setParallelism(configuration.get(Configs.COMBINER_WINDOW_PARALLELISM))
                            .union(fileMetaSessionSingleOutputStreamOperator, fileMetaProxySingleOutputStreamOperator)
                            .keyBy(new FileChunkKeySelector())
                            .addSink(new OssSinkByEhcache(configuration))
                            .name("Oss")
                            .setParallelism(configuration.get(Configs.SINK_PARALLELISM));
                    break;
                case "oss-caffeine":
                    fileMetaSessionSingleOutputStreamOperator = environment.addSource(FileMetaKafkaConsumer.stringConsumer(configuration, configuration.get(Configs.KAFKA_FILE_META_SESSION_TOPIC)))
                            .setParallelism(configuration.get(Configs.MAP_PARSE_FILE_META_PARALLELISM))
                            .name(configuration.get(Configs.KAFKA_FILE_META_SESSION_TOPIC))
                            .flatMap(new ParseSessionFileMetaFlatMapFunction())
                            .setParallelism(configuration.get(Configs.MAP_PARSE_FILE_META_PARALLELISM))
                            .name("Map: Parse Session File Meta")
                            .filter(new FileChunkFilterFunction(configuration.getString(Configs.FILE_META_FILTER_EXPRESSION), "map_parse_session_file_meta"))
                            .name("Filter: Map")
                            .setParallelism(configuration.get(Configs.MAP_PARSE_FILE_META_PARALLELISM));
                    fileMetaProxySingleOutputStreamOperator = environment.addSource(FileMetaKafkaConsumer.stringConsumer(configuration, configuration.get(Configs.KAFKA_FILE_META_PROXY_TOPIC)))
                            .setParallelism(configuration.get(Configs.MAP_PARSE_FILE_META_PARALLELISM))
                            .name(configuration.get(Configs.KAFKA_FILE_META_PROXY_TOPIC))
                            .flatMap(new ParseProxyFileMetaFlatMapFunction())
                            .setParallelism(configuration.get(Configs.MAP_PARSE_FILE_META_PARALLELISM))
                            .name("Map: Parse Proxy File Meta")
                            .filter(new FileChunkFilterFunction(configuration.getString(Configs.FILE_META_FILTER_EXPRESSION), "map_parse_proxy_file_meta"))
                            .name("Filter: Map")
                            .setParallelism(configuration.get(Configs.MAP_PARSE_FILE_META_PARALLELISM));
                    windowStream
                            .filter(new FileChunkFilterFunction(configuration.getString(Configs.SINK_OSS_FILTER_EXPRESSION), "sink_oss"))
                            .name("Filter: Oss")
                            .setParallelism(configuration.get(Configs.COMBINER_WINDOW_PARALLELISM))
                            .union(fileMetaSessionSingleOutputStreamOperator, fileMetaProxySingleOutputStreamOperator)
                            .keyBy(new FileChunkKeySelector())
                            .addSink(new OssSinkByCaffeineCache(configuration))
                            .name("Oss")
                            .setParallelism(configuration.get(Configs.SINK_PARALLELISM));
                    break;
                case "test":
                    fileMetaSessionSingleOutputStreamOperator = environment.addSource(FileMetaKafkaConsumer.stringConsumer(configuration, configuration.get(Configs.KAFKA_FILE_META_SESSION_TOPIC)))
                            .setParallelism(configuration.get(Configs.MAP_PARSE_FILE_META_PARALLELISM))
                            .name(configuration.get(Configs.KAFKA_FILE_META_SESSION_TOPIC))
                            .flatMap(new ParseSessionFileMetaFlatMapFunction())
                            .setParallelism(configuration.get(Configs.MAP_PARSE_FILE_META_PARALLELISM))
                            .name("Map: Parse Session File Meta")
                            .filter(new FileChunkFilterFunction(configuration.getString(Configs.FILE_META_FILTER_EXPRESSION), "map_parse_session_file_meta"))
                            .name("Filter: Map")
                            .setParallelism(configuration.get(Configs.MAP_PARSE_FILE_META_PARALLELISM));
                    fileMetaProxySingleOutputStreamOperator = environment.addSource(FileMetaKafkaConsumer.stringConsumer(configuration, configuration.get(Configs.KAFKA_FILE_META_PROXY_TOPIC)))
                            .setParallelism(configuration.get(Configs.MAP_PARSE_FILE_META_PARALLELISM))
                            .name(configuration.get(Configs.KAFKA_FILE_META_PROXY_TOPIC))
                            .flatMap(new ParseProxyFileMetaFlatMapFunction())
                            .setParallelism(configuration.get(Configs.MAP_PARSE_FILE_META_PARALLELISM))
                            .name("Map: Parse Proxy File Meta")
                            .filter(new FileChunkFilterFunction(configuration.getString(Configs.FILE_META_FILTER_EXPRESSION), "map_parse_proxy_file_meta"))
                            .name("Filter: Map")
                            .setParallelism(configuration.get(Configs.MAP_PARSE_FILE_META_PARALLELISM));
                    KeyedStream<FileChunk, String> fileMetaStringKeyedStream = fileMetaSessionSingleOutputStreamOperator
                            .union(fileMetaProxySingleOutputStreamOperator)
                            .keyBy((KeySelector<FileChunk, String>) FileChunk::getUuid);
                    windowStream
                            .filter(new FileChunkFilterFunction(configuration.getString(Configs.SINK_OSS_FILTER_EXPRESSION), "sink_oss"))
                            .name("Filter: Oss")
                            .setParallelism(configuration.get(Configs.COMBINER_WINDOW_PARALLELISM))
                            .keyBy((KeySelector<FileChunk, String>) FileChunk::getUuid)
                            .connect(fileMetaStringKeyedStream)
                            .process(new TestKeyedCoProcessFunction(configuration))
                            .setParallelism(configuration.get(Configs.COMBINER_WINDOW_PARALLELISM))
                            .name("Oss");
                    break;
            }
        }
        environment.execute(configuration.get(Configs.FLINK_JOB_NAME));
    }
}
