package com.zdjizhi;

import com.zdjizhi.config.Configs;
import com.zdjizhi.function.*;
import com.zdjizhi.pojo.*;
import com.zdjizhi.sink.HBaseSink;
import com.zdjizhi.sink.HosSink;
import com.zdjizhi.kafka.KafkaConsumer;
import com.zdjizhi.trigger.LastChunkOrNoDataInTimeTrigger;
import com.zdjizhi.trigger.MultipleTrigger;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class FileChunkCombiner {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);
        final Configuration configuration = parameterTool.getConfiguration();
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().setGlobalJobParameters(configuration);

        WatermarkStrategy<FileChunk> watermarkStrategy = WatermarkStrategy
                .<FileChunk>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((FileChunk, timestamp) -> FileChunk.getTimestamp() / 1000);

        SingleOutputStreamOperator<FileChunk> parseMessagePackStream = environment
                .addSource(KafkaConsumer.byteArrayConsumer(configuration))
                .name("Kafka Source")
                .map(new ParseMessagePackMapFunction())
                .name("Map: Parse Message Pack")
                .filter(new FileChunkFilter(configuration.getLong(Configs.FILE_MAX_SIZE), configuration.getString(Configs.FILTER_EXPRESSION)))
                .assignTimestampsAndWatermarks(watermarkStrategy);

        OutputTag<FileChunk> delayedChunkOutputTag = new OutputTag<FileChunk>("delayed-chunk") {
        };

        List<Trigger<Object, TimeWindow>> triggers = new ArrayList<>();
        triggers.add(EventTimeTrigger.create());
        triggers.add(LastChunkOrNoDataInTimeTrigger.of(configuration.get(Configs.COMBINER_WINDOW_IDLE_TIME) * 1000));
        Trigger<Object, TimeWindow> trigger = MultipleTrigger.of(triggers);
        SingleOutputStreamOperator<FileChunk> windowStream = parseMessagePackStream
                .keyBy(new FileChunkKeySelector(), BasicTypeInfo.STRING_TYPE_INFO)
                .window(TumblingEventTimeWindows.of(Time.seconds(configuration.get(Configs.COMBINER_WINDOW_TIME))))
                .trigger(trigger)
                .sideOutputLateData(delayedChunkOutputTag)
                .process(new CombineChunkProcessWindowFunction(configuration))
                .name("Window: Combine Chunk")
                .setParallelism(configuration.get(Configs.COMBINER_WINDOW_PARALLELISM))
                .disableChaining();

        if ("hos".equals(configuration.get(Configs.SINK_TYPE))) {
            windowStream.addSink(new HosSink(configuration))
                    .name("Hos")
                    .setParallelism(configuration.get(Configs.SINK_PARALLELISM));
            windowStream.getSideOutput(delayedChunkOutputTag)
                    .map(new SideOutputMapFunction())
                    .addSink(new HosSink(configuration))
                    .name("Hos Delayed Chunk");
        } else {
            windowStream.addSink(new HBaseSink(configuration))
                    .name("HBase")
                    .setParallelism(configuration.get(Configs.SINK_PARALLELISM));
            windowStream.getSideOutput(delayedChunkOutputTag)
                    .map(new SideOutputMapFunction())
                    .addSink(new HBaseSink(configuration))
                    .name("HBase Delayed Chunk");
        }

        environment.execute(configuration.get(Configs.FLINK_JOB_NAME));
    }

}