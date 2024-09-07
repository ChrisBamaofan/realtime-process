package com.zetyun.hqbank.example.chatgptExamples;

import com.zetyun.hqbank.bean.ClickEvents;
import com.zetyun.hqbank.bean.Riches;
import com.zetyun.hqbank.config.ClickEventsDeserializationSchema;
import com.zetyun.hqbank.config.RichesDeserializationSchema;
import com.zetyun.hqbank.example.TopN.RichesAggregation;
import com.zetyun.hqbank.example.TopN.RichesWindowFunction;
import com.zetyun.hqbank.example.TopN.SortProcessFunction;
import com.zetyun.hqbank.util.YamlUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

public class ClickExample {

    /***
     * user1 urls-100 100
     * user2 urls-12 100
     * user3 urls-10 100
     *
     */
    public static void main(String[] args) throws Exception {
        String systemConfigPath = "D:/conf/windows/systemConfig.yaml";
//        String systemConfigPath = "/opt/flink1.15/config/systemConfig.yaml";
        String bootstrap = YamlUtil.getValueByKey(systemConfigPath, "kafka", "bootstrap");

        Configuration flinkConfig = new Configuration();
        flinkConfig.setInteger(RestOptions.PORT, 8888);
        flinkConfig.setBoolean("rest.flamegraph.enabled",true);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig).disableOperatorChaining().setParallelism(2);


        Properties sourceProps = new Properties();
        sourceProps.setProperty("group.id", "g1");
        sourceProps.setProperty("scan.startup.mode", "latest-offset");

        KafkaSource<ClickEvents> source = KafkaSource.<ClickEvents>builder()
                .setBootstrapServers(bootstrap)
                .setTopics("test7")
                .setGroupId("g1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new ClickEventsDeserializationSchema())
                .setProperties(sourceProps)
                .build();

        WatermarkStrategy<ClickEvents> watermarkStrategy = WatermarkStrategy
                .<ClickEvents>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((SerializableTimestampAssigner<ClickEvents>) (clickEvents, l) -> clickEvents.getTs() * 1000L);

        DataStreamSource<ClickEvents> kafkaSource = env.fromSource(source, watermarkStrategy, "kafkaSource");

        SingleOutputStreamOperator<String> aggregate = kafkaSource.keyBy(ClickEvents::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new Agg());

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("test2")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        aggregate.sinkTo(sink);
        env.execute("window start job");
    }

    private static class Agg implements AggregateFunction<ClickEvents, HashSet<String>,String>{

        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(ClickEvents clickEvents, HashSet<String> strings) {
            strings.add(clickEvents.getUrl());

            return strings;
        }

        @Override
        public String getResult(HashSet<String> strings) {
            return String.valueOf(strings.size());
        }

        @Override
        public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
            boolean b = strings.addAll(acc1);
            return strings;
        }
    }

}
