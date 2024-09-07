package com.zetyun.hqbank.example.timeAndWindow;

import com.zetyun.hqbank.bean.Student;
import com.zetyun.hqbank.config.StudentDeserializationSchema;
import com.zetyun.hqbank.util.YamlUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MixWindowFuncExample {
    private static final Logger logger = LoggerFactory.getLogger(MixWindowFuncExample.class);
    private static final String TOPIC_1 = "test1";
    private static final String TOPIC_2 = "test2";

    public static void main(String[] args) throws Exception {
        String systemConfigPath = "D:/conf/windows/systemConfig.yaml";
//        String systemConfigPath = "/opt/flink1.15/config/systemConfig.yaml";

        String bootstrap = YamlUtil.getValueByKey(systemConfigPath, "kafka", "bootstrap");


        Configuration flinkConfig = new Configuration();
        flinkConfig.setInteger(RestOptions.PORT, 8888);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig).setParallelism(2).disableOperatorChaining();
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
        KeyedStream<Student, Integer> keyedStream = sourceStream.keyBy((KeySelector<Student, Integer>) Student::getId);

        // 每十秒 统计 进入学校的同学人数
        SingleOutputStreamOperator<Integer> aggregate = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5))).aggregate(new Aggre(), new ProcessWindowFunction<Integer, Integer, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<Integer> iterable, Collector<Integer> collector) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        System.out.println("key is "+integer+" window start time "+start+" window end time "+end+" 内容是 "+iterable.toString());
                    }
                }
        ) ;

//        KafkaSink<String> sink = KafkaSink.<String>builder()
//                .setBootstrapServers(bootstrap)
//                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//                        .setTopic(TOPIC_2)
//                        .setValueSerializationSchema(new SimpleStringSchema())
//                        .build()
//                )
//                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                .build();
//
//        aggregate.sinkTo(sink);
        env.execute("window start job");
    }

    public static class Aggre implements AggregateFunction<Student, Integer, Integer>{
        @Override
        public Integer createAccumulator() {
            // 初始化累加器
            return 0;
        }

        @Override
        public Integer add(Student student, Integer acc) {
            // 累加器更新逻辑，这里简单地增加 1
            return student.getGrade() + acc;
        }

        @Override
        public Integer getResult(Integer acc) {
            // 窗口结束时，返回累加器的值作为结果
            return acc;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            // 如果你的 Flink 作业在并行环境下运行，这个方法会被用来合并来自不同并行子任务的累加器
            // 这里也是简单地相加
            return a + b;
        }
    }

}