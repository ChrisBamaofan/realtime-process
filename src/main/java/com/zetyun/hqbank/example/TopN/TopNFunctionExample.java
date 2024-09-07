package com.zetyun.hqbank.example.TopN;

import com.zetyun.hqbank.bean.Riches;
import com.zetyun.hqbank.bean.Student;
import com.zetyun.hqbank.config.RichesDeserializationSchema;
import com.zetyun.hqbank.config.StudentDeserializationSchema;
import com.zetyun.hqbank.util.YamlUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static com.zetyun.hqbank.util.TimeUtils.timestamToDatetime;

public class TopNFunctionExample {

    /***
     * user1 250(w) 100
     * user2 150(w) 100
     * user3 100(w) 100
     *
     */
    public static void main(String[] args) throws Exception {
        String systemConfigPath = "D:/conf/windows/systemConfig.yaml";
//        String systemConfigPath = "/opt/flink1.15/config/systemConfig.yaml";
        String bootstrap = YamlUtil.getValueByKey(systemConfigPath, "kafka", "bootstrap");

        Configuration flinkConfig = new Configuration();
        flinkConfig.setInteger(RestOptions.PORT, 8888);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig).setParallelism(1);


        Properties sourceProps = new Properties();
        sourceProps.setProperty("group.id", "g1");
        sourceProps.setProperty("scan.startup.mode", "latest-offset");

        KafkaSource<Riches> source = KafkaSource.<Riches>builder()
                .setBootstrapServers(bootstrap)
                .setTopics("test7")
                .setGroupId("g1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new RichesDeserializationSchema())
                .setProperties(sourceProps)
                .build();

        WatermarkStrategy<Riches> studentWatermarkStrategy =
                WatermarkStrategy.<Riches>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((SerializableTimestampAssigner<Riches>) (riches, l) -> {
                    System.out.println("get data from ="+riches+" l = "+l);
                    return riches.getTs()*1000L;
                });

        // 计算获取到的事件中 某种类型的出现次数的 top 2
        DataStreamSource<Riches> sourceStream = env.fromSource(source,studentWatermarkStrategy , "Kafka Source");

        //
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> aggregate = sourceStream
                .keyBy((KeySelector<Riches, Integer>) Riches::getId)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                // 先聚合 ，再通过窗口获取相应的窗口上下文信息，再将窗口结束时间以及对应的财富值输出
                .aggregate(new RichesAggregation(), new RichesWindowFunction());

        // 对每个窗口结束时间为标志的分组，对每一组排序，返回前两名
        SingleOutputStreamOperator<String> process = aggregate
                .keyBy((KeySelector<Tuple3<Integer, Integer, Long>, Long>) tp -> tp.f2)
                .process(new SortProcessFunction(2));

        process.print();
        env.execute("window start job");
    }

}
