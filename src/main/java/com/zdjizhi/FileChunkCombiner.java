package com.zdjizhi;

import cn.hutool.core.util.StrUtil;
import com.zdjizhi.config.Configs;
import com.zdjizhi.function.*;
import com.zdjizhi.function.map.ParseMessagePackMapFunction;
import com.zdjizhi.function.map.ParseProxyFileMetaFlatMapFunction;
import com.zdjizhi.function.map.ParseSessionFileMetaFlatMapFunction;
import com.zdjizhi.kafka.FileMetaKafkaConsumer;
import com.zdjizhi.pojo.*;
import com.zdjizhi.sink.*;
import com.zdjizhi.kafka.KafkaConsumer;
import com.zdjizhi.trigger.LastChunkTrigger;
import com.zdjizhi.trigger.MultipleTrigger;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.*;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.*;

public class FileChunkCombiner {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);
        final Configuration configuration = parameterTool.getConfiguration();
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().setGlobalJobParameters(configuration);

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
        SingleOutputStreamOperator<FileChunk> windowStream = parseMessagePackStream
                .keyBy(new FileChunkKeySelector(), BasicTypeInfo.STRING_TYPE_INFO)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(configuration.get(Configs.COMBINER_WINDOW_SIZE))))
                .trigger(trigger)
                .process(new CombineChunkProcessWindowFunction())
                .name("Window: Combine Chunk")
                .setParallelism(configuration.get(Configs.COMBINER_WINDOW_PARALLELISM));

        SingleOutputStreamOperator<FileChunk> fileMetaSessionSingleOutputStreamOperator;
        SingleOutputStreamOperator<FileChunk> fileMetaProxySingleOutputStreamOperator;
        for (String sinkType : configuration.get(Configs.SINK_TYPE).split(",")) {
            switch (sinkType) {
                case "hos":
                    if (StrUtil.isNotEmpty(configuration.getString(Configs.SINK_FILTER_EXPRESSION))) {
                        windowStream
                                .filter(new FileChunkFilterFunction(configuration.getString(Configs.SINK_FILTER_EXPRESSION), "sink_hos"))
                                .name("Filter: Hos")
                                .setParallelism(configuration.get(Configs.SINK_PARALLELISM))
                                .addSink(new HosSink(configuration))
                                .name("Hos")
                                .setParallelism(configuration.get(Configs.SINK_PARALLELISM));
                    } else {
                        windowStream
                                .addSink(new HosSink(configuration))
                                .name("Hos")
                                .setParallelism(configuration.get(Configs.SINK_PARALLELISM));
                    }
                    break;
                case "hbase":
                    if (StrUtil.isNotEmpty(configuration.getString(Configs.SINK_FILTER_EXPRESSION))) {
                        windowStream
                                .filter(new FileChunkFilterFunction(configuration.getString(Configs.SINK_FILTER_EXPRESSION), "sink_hbase"))
                                .name("Filter: HBase")
                                .setParallelism(configuration.get(Configs.SINK_PARALLELISM))
                                .addSink(new HBaseSink(configuration))
                                .name("HBase")
                                .setParallelism(configuration.get(Configs.SINK_PARALLELISM));
                    } else {
                        windowStream
                                .addSink(new HBaseSink(configuration))
                                .name("HBase")
                                .setParallelism(configuration.get(Configs.SINK_PARALLELISM));
                    }
                    break;
                case "oss":
                    fileMetaSessionSingleOutputStreamOperator = environment.addSource(FileMetaKafkaConsumer.stringConsumer(configuration, configuration.get(Configs.KAFKA_FILE_META_SESSION_TOPIC)))
                            .name(configuration.get(Configs.KAFKA_FILE_META_SESSION_TOPIC))
                            .flatMap(new ParseSessionFileMetaFlatMapFunction())
                            .name("Map: Parse Session File Meta")
                            .filter(new FileChunkFilterFunction(configuration.getString(Configs.FILE_META_FILTER_EXPRESSION), "map_parse_session_file_meta"))
                            .name("Filter: Map");
                    fileMetaProxySingleOutputStreamOperator = environment.addSource(FileMetaKafkaConsumer.stringConsumer(configuration, configuration.get(Configs.KAFKA_FILE_META_PROXY_TOPIC)))
                            .name(configuration.get(Configs.KAFKA_FILE_META_PROXY_TOPIC))
                            .flatMap(new ParseProxyFileMetaFlatMapFunction())
                            .name("Map: Parse Proxy File Meta")
                            .filter(new FileChunkFilterFunction(configuration.getString(Configs.FILE_META_FILTER_EXPRESSION), "map_parse_proxy_file_meta"))
                            .name("Filter: Map");
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
            }
        }
        environment.execute(configuration.get(Configs.FLINK_JOB_NAME));
    }
}
