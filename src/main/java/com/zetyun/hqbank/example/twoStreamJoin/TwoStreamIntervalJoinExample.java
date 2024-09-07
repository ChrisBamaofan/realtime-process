package com.zetyun.hqbank.example.twoStreamJoin;

import com.zetyun.hqbank.bean.Student;
import com.zetyun.hqbank.config.StudentDeserializationSchema;
import com.zetyun.hqbank.util.YamlUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class TwoStreamIntervalJoinExample {
    private static final Logger logger = LoggerFactory.getLogger(TwoStreamIntervalJoinExample.class);
    private static final String TOPIC_1 = "test11";
    private static final String TOPIC_2 = "test2";

    public static void main(String[] args) throws Exception {
        String systemConfigPath = "D:/conf/windows/systemConfig.yaml";
//        String systemConfigPath = "/opt/flink1.15/config/systemConfig.yaml";

        String bootstrap = YamlUtil.getValueByKey(systemConfigPath, "kafka", "bootstrap");


        Configuration flinkConfig = new Configuration();
        flinkConfig.setInteger(RestOptions.PORT, 8888);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig).setParallelism(2);
//        env.enableCheckpointing(checkpointInterval);

        String sourceTopic = TOPIC_1;
        String sinkTopic = TOPIC_2;

        Properties sourceProps = new Properties();
        sourceProps.setProperty("group.id", "g1");
        sourceProps.setProperty("scan.startup.mode", "latest-offset");

        KafkaSource<Student> source1 = KafkaSource.<Student>builder()
                .setBootstrapServers(bootstrap)
                .setTopics("test5")
                .setGroupId("g1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new StudentDeserializationSchema())
                .setProperties(sourceProps)
                .build();

        KafkaSource<Student> source2 = KafkaSource.<Student>builder()
                .setBootstrapServers(bootstrap)
                .setTopics("test6")
                .setGroupId("g1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new StudentDeserializationSchema())
                .setProperties(sourceProps)
                .build();


        // 乱序流
        WatermarkStrategy<Student> studentWatermarkStrategy =
                WatermarkStrategy.<Student>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<Student>(){

                    @Override
                    public long extractTimestamp(Student student, long l) {
                        System.out.println("数据="+student+" l = "+l);
                        return student.getTs()*1000L;
                    }
                });

        KeyedStream<Student, Object> sourceStream1 = env.fromSource(source1, studentWatermarkStrategy, "Kafka Source" + sourceTopic).keyBy((KeySelector<Student, Object>) Student::getId);
        KeyedStream<Student, Object>  sourceStream2 = env.fromSource(source2,studentWatermarkStrategy , "Kafka Source" + sourceTopic).keyBy((KeySelector<Student, Object>) Student::getId);
         sourceStream1.intervalJoin(sourceStream2)
                .between(Time.seconds(3),Time.seconds(3))
                .process(new ProcessJoinFunction<Student, Student, Object>() {
            @Override
            public void processElement(Student student, Student student2, Context context, Collector<Object> collector) throws Exception {
                String s = student.getName() + student2.getName();
                System.out.println(s);
                collector.collect(s);
            }
        });

        env.execute("window start job");
    }

}
