package com.zetyun.hqbank.example.watermark;

import com.zetyun.hqbank.bean.Student;
import com.zetyun.hqbank.config.StudentDeserializationSchema;
import com.zetyun.hqbank.util.YamlUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

import static com.zetyun.hqbank.util.TimeUtils.timestamToDatetime;

public class WaterMarkWindowFuncExample {
    private static final Logger logger = LoggerFactory.getLogger(WaterMarkWindowFuncExample.class);
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

        KafkaSource<Student> source = KafkaSource.<Student>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(sourceTopic)
                .setGroupId("g1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new StudentDeserializationSchema())
                .setProperties(sourceProps)
                .build();

// 有序流
//        WatermarkStrategy<Student> studentWatermarkStrategy =
//                WatermarkStrategy.<Student>forMonotonousTimestamps().withTimestampAssigner(
//                new SerializableTimestampAssigner<Student>() {
//                    @Override
//                    public long extractTimestamp(Student student, long l) {
//                        System.out.println("数据="+student+" l = "+l);
//                        return student.getTs()*1000L;
//                    }
//                });
        // 乱序流
        WatermarkStrategy<Student> studentWatermarkStrategy =
                WatermarkStrategy.<Student>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<Student>(){

                    @Override
                    public long extractTimestamp(Student student, long l) {
                        System.out.println("数据="+student+" l = "+l);
                        return student.getTs()*1000L;
                    }
                });

        DataStreamSource<Student> sourceStream = env.fromSource(source,studentWatermarkStrategy , "Kafka Source" + sourceTopic);
        SingleOutputStreamOperator<Integer> process = sourceStream.keyBy((KeySelector<Student, Integer>) Student::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(
                        new ProcessWindowFunction<Student, Integer, Integer, TimeWindow>() {
                            @Override
                            public void process(Integer integer, Context context, Iterable<Student> iterable, Collector<Integer> collector) throws Exception {
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                System.out.println("当前水位线: " + context.currentWatermark());
                                System.out.println("key is " + integer + " window start time " + timestamToDatetime(start) + " window end time " + timestamToDatetime(end) + " 内容是 " + iterable.toString());
                            }
                        }
                );
        ;

        process.print();
        env.execute("window start job");
    }

}
