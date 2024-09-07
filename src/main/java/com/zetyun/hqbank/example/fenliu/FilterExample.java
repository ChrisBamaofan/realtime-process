package com.zetyun.hqbank.example.fenliu;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zetyun.hqbank.bean.Student;
import com.zetyun.hqbank.config.StudentDeserializationSchema;
import com.zetyun.hqbank.util.YamlUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 从雪球爬数据并每隔10分钟捞取数据，并调用模型做预测，并将结果写入kafka
 */
public class FilterExample {
    private static final Logger logger = LoggerFactory.getLogger(FilterExample.class);
    private static final String TOPIC_1 = "test1";
    private static final String TOPIC_2 = "test2";
    private static final String TOPIC_3 = "test3";
    private static final String TOPIC_4 = "test4";

    public static void main(String[] args) throws Exception {
        String systemConfigPath = "D:/conf/windows/systemConfig.yaml";
//        String systemConfigPath = "/opt/flink1.15/config/systemConfig.yaml";

        String bootstrap = YamlUtil.getValueByKey(systemConfigPath, "kafka", "bootstrap");


        Configuration flinkConfig = new Configuration();
        flinkConfig.setInteger(RestOptions.PORT, 8888);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig).setParallelism(1).disableOperatorChaining();
//        env.enableCheckpointing(checkpointInterval);

        String sourceTopic = TOPIC_1;
        String sinkTopic = TOPIC_2;

        Properties sourceProps = new Properties();
        sourceProps.setProperty("group.id", "g1");
        sourceProps.setProperty("scan.startup.mode", "latest-offset");

        KafkaSource<Student> source = KafkaSource.<Student>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(sourceTopic)
                .setGroupId("g1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new StudentDeserializationSchema())
                .setProperties(sourceProps)
                .build();

        DataStreamSource<Student> sourceStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source" + sourceTopic);

        // process after keyBy
//        SingleOutputStreamOperator<String> process = sourceStream.keyBy((KeySelector<Student, Integer>) Student::getId)
//                .process(new SimpleKeyProcessFunc());

        // 分流
        SingleOutputStreamOperator<String> highStream = sourceStream.filter((FilterFunction<Student>) student -> student.getGrade()>80).map((MapFunction<Student, String>) student -> {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsString(student);
        });
        SingleOutputStreamOperator<String> lowStream = sourceStream.filter((FilterFunction<Student>) student -> student.getGrade()<=80).map((MapFunction<Student, String>) student -> {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsString(student);
        });

        DataStream<String> union = highStream.union(lowStream);
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(TOPIC_2)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        KafkaSink<String> sink2 = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(TOPIC_3)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        KafkaSink<String> sink3 = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(TOPIC_4)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        highStream.sinkTo(sink);
        lowStream.sinkTo(sink2);
//        OutputTag
        union.sinkTo(sink3);
        env.execute("同步数据作业A");
    }

    // 根据key 做 对应操作，
    public static class SimpleKeyProcessFunc extends KeyedProcessFunction<Integer,Student, String>{

        private transient ValueState<Integer> lastReading;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("lastReading", Integer.class);
            lastReading = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Student input, Context context, Collector<String> collector) throws Exception {
            Integer last = lastReading.value();
            if (last == null) {
                // 如果是第一次读取，不产生警告
                lastReading.update(input.getGrade());
            } else if (input.getGrade() - last > 0) {
                // 如果当前值与上次值相差超过0.5，则发出警告
                collector.collect("good: " + input.getName() + " 现在成绩是 " + input.getGrade()+" 上次成绩是 "+ lastReading.value());
                lastReading.update(input.getGrade());
            }else if (input.getGrade() - last < 0) {
                // 如果当前值与上次值相差超过0.5，则发出警告
                collector.collect("bad: " + input.getName() + " 成绩是 " + input.getGrade()+" 上次成绩是 "+ lastReading.value());
                lastReading.update(input.getGrade());
            }else if (input.getGrade() - last == 0) {
                // 如果当前值与上次值相差超过0.5，则发出警告
                collector.collect("ok : " + input.getName() + " 成绩是 " + input.getGrade()+" 上次成绩是 "+ lastReading.value());
                lastReading.update(input.getGrade());
            }

        }

//        @Override
//        public void close(){
//
//        }
    }


}
