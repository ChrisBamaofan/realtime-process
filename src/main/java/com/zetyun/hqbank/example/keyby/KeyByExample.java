package com.zetyun.hqbank.example.keyby;

import com.zetyun.hqbank.bean.Student;
import com.zetyun.hqbank.config.StudentDeserializationSchema;
import com.zetyun.hqbank.util.YamlUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
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
public class KeyByExample {
    private static final Logger logger = LoggerFactory.getLogger(KeyByExample.class);
    private static final String TOPIC_1 = "test1";
    private static final String TOPIC_2 = "test2";

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

        // 两两聚合操作
        SingleOutputStreamOperator<Student> process = sourceStream.keyBy((KeySelector<Student, Integer>) Student::getId).reduce(new ReduceFunction<Student>() {
            @Override
            public Student reduce(Student student, Student t1) throws Exception {
                System.out.print("s1= "+student+" ");
                System.out.print("s2= "+t1+" ");
                student.setGrade(student.getGrade()+t1.getGrade());
                return student;
            }
        });

        process.print();
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
