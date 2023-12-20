package com.zetyun.hqbank;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zetyun.hqbank.bean.dds.DDSData;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * @author zhaohaojie
 * @date 2023-12-20 10:32
 */
public class DDS2FlinkCDCTest {
    private static final Logger logger = LoggerFactory.getLogger(DDS2FlinkCDCTest.class);

    public static void main(String[] args) throws Exception {
        // 设置 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置 Kafka 源相关参数
        Properties sourceProps = new Properties();
        sourceProps.setProperty("bootstrap.servers", "172.20.29.5:19092");
        sourceProps.setProperty("group.id", "g1");

        // 创建 Kafka 源数据流
        DataStream<String> sourceStream = env.addSource(new FlinkKafkaConsumer<>(
                "dds_cdc_source",
                new SimpleStringSchema(),
                sourceProps
        ));

        // 对每条数据进行反序列化和处理，添加字段name
        DataStream<String> processedStream = sourceStream.map(data -> {
            logger.info("==> get data from kafka [get crud] :{}",data);
            String processedData = processData(data);

            // 返回处理后的数据
            return processedData;
        });

        SingleOutputStreamOperator<String> map = sourceStream.map(data -> {
            logger.info("==> get data from kafka [get schema] :{}", data);
            String processedData = processSchema(data);

            // 返回处理后的数据
            return processedData;
        });

        // 设置 Kafka 宿相关参数
        Properties sinkProps = new Properties();
        sinkProps.setProperty("bootstrap.servers", "172.20.29.5:19092");

        // 创建 Kafka 宿数据流
        processedStream.addSink(new FlinkKafkaProducer<>(
                "dds_cdc_sink",
                new SimpleStringSchema(),
                sinkProps
        ));

        // 执行程序
        env.execute("Kafka Data Processing");
    }

    private static String processSchema(String data) {

        return null;
    }

    private static String processData(String input) {
        ObjectMapper om = new ObjectMapper();
        try {
            DDSData ddsData = om.readValue(input, DDSData.class);
            return om.writeValueAsString(ddsData.getPayload());
        } catch (IOException e) {
            logger.error("异常情况！",e);
        }
        return "";
    }

}

