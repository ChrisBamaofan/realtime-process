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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

import static com.zetyun.hqbank.util.TimeUtils.timestamToDatetime;

public class TwoStreamJoinExample {
    private static final Logger logger = LoggerFactory.getLogger(TwoStreamJoinExample.class);
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

        DataStreamSource<Student> sourceStream1 = env.fromSource(source1,studentWatermarkStrategy , "Kafka Source" + sourceTopic);
        DataStreamSource<Student> sourceStream2 = env.fromSource(source2,studentWatermarkStrategy , "Kafka Source" + sourceTopic);
        DataStream<Object> apply = sourceStream1.join(sourceStream2)
                .where((KeySelector<Student, Object>) Student::getId)
                .equalTo((KeySelector<Student, Object>) Student::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply((JoinFunction<Student, Student, Object>) (student, student2) -> {
                    String s = student.getName() + student2.getName();
                    System.out.println(s);
                    return s;
                });
        apply.print();

        env.execute("window start job");
    }

}
