package com.zetyun.hqbank;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import com.zetyun.hqbank.util.YamlUtil;
import com.zetyun.hqbank.bean.dds.DDSData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DDS2FlinkCDC {
    private static Logger logger = LoggerFactory.getLogger(DDS2FlinkCDC.class);

    public static void main(String[] args) {

        // 判断DDS是 insert/update/delete
        // 执行相应的 语句
        String hiveUri = "thrift://172.20.1.34:9083";
        String warehouse = "hdfs://172.20.1.34:8020/user/hive/warehouse/";
        String hive_conf_dir = "/Users/lifenghua/src/iceberg-demo/config/hive-conf-34";
        String hadoop_conf_dir = "/Users/lifenghua/src/iceberg-demo/config/hive-conf-34";
        // 1 .1 读取配置文件
        // 2. 获取kafka输入流
        List<String> topics = YamlUtil.getListByKey("application.yaml", "kafka", "topic");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("172.20.29.5")
                .setTopics("topic1")//todo 读取配置文件
                .setGroupId("group1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");


        SingleOutputStreamOperator<Object> tableStream = kafkaSource.map(data -> {
            ObjectMapper objectMapper = new ObjectMapper();
            DDSData ddsPayload = objectMapper.readValue(data, DDSData.class);
            // 针对该topic 的iceberg表 作 update,delete,insert
            if (StringUtils.equals(ddsPayload.getPayload().getOp(), "u")) {

            } else if (StringUtils.equals(ddsPayload.getPayload().getOp(), "d")) {

            } else if (StringUtils.equals(ddsPayload.getPayload().getOp(), "c")) {

            }
            return null;
        });

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env, settings);

    }


}
