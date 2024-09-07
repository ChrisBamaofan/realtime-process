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
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CountWindowExample {
    private static final Logger logger = LoggerFactory.getLogger(CountWindowExample.class);
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
        KeyedStream<Student, Integer> keyedStream = sourceStream.setParallelism(2).keyBy((KeySelector<Student, Integer>) Student::getId);
        keyedStream.countWindow(3,1).process(new ProcessWindowFunction<Student, Object, Integer, GlobalWindow>() {
            @Override
            public void process(Integer integer, Context context, Iterable<Student> iterable, Collector<Object> collector) throws Exception {
                System.out.println("key="+integer+" context="+context.window().maxTimestamp()+" 包含 "+iterable.spliterator().estimateSize()+" 个元素 为 "+iterable.toString());

            }
        });

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


}
