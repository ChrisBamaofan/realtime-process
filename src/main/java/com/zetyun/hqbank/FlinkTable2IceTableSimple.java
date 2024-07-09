package com.zetyun.hqbank;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zetyun.hqbank.bean.dds.DDSData;
import com.zetyun.hqbank.bean.dds.DDSPayload;
import com.zetyun.hqbank.bean.flink.FlinkTableMap;
import com.zetyun.hqbank.enums.DDSOprEnums;
import com.zetyun.hqbank.service.oracle.OracleService;
import com.zetyun.hqbank.util.KafkaUtil;
import com.zetyun.hqbank.util.YamlUtil;
import com.zetyun.rt.jasyptwrapper.Jasypt;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

import static com.zetyun.hqbank.bean.dds.DDSPayload.encrypt;

public class FlinkTable2IceTableSimple {
    private static final Logger logger = LoggerFactory.getLogger(DDS2FlinkCDCSimple.class);
    private ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String userConfigPath = parameters.get("userConfig");
        String systemConfigPath = "D:/conf/windows/systemConfig.yaml";

        String bootstrap = YamlUtil.getValueByKey(systemConfigPath, "kafka", "bootstrap");

        logger.info("bootstrap:{}", bootstrap);

        Long checkpointInterval = Long.valueOf(YamlUtil.getValueByKey(userConfigPath, "flink", "checkpointInterval"));

        Configuration flinkConfig = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
        env.enableCheckpointing(checkpointInterval);


        String sourceTopic = "target-topic";
        String sinkTopic = "target222-topic";
        // 设置 Kafka 源相关参数
        Properties sourceProps = new Properties();
        sourceProps.setProperty("group.id", "g1");
        sourceProps.setProperty("scan.startup.mode", "latest-offset");

        // 创建 Kafka 源数据流
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(sourceTopic)
                .setGroupId("g1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(sourceProps)
                .build();

        DataStreamSource<String> sourceStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source" + sourceTopic);

        // 对每条数据进行反序列化和处理
        DataStream<String> processedStream = sourceStream.map(data -> {
            logger.info("==> get data from kafka [get crud] :{}", data);
            return data;
        });
        // 设置 Kafka 宿相关参数
        logger.info("==> 从源topic:{}->宿topic:{}", sourceTopic, sinkTopic);
        // 创建 Kafka 宿数据流
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(sinkTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        processedStream.sinkTo(sink);
        env.execute("同步数据作业A：");
    }

}
