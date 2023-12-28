package com.zetyun.hqbank;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zetyun.hqbank.bean.dds.DDSData;
import com.zetyun.hqbank.service.oracle.OracleService;
import com.zetyun.hqbank.util.YamlUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.zetyun.hqbank.service.oracle.OracleService.CONFIG_PATH;

/**
 * @author zhaohaojie
 * @date 2023-12-20 10:32
 */
public class DDS2FlinkCDC {
    private static final Logger logger = LoggerFactory.getLogger(DDS2FlinkCDC.class);
    public static final List<String> whiteList = Arrays.asList(new String[]{"orcl-dds-t_zhj2"});

    public static void main(String[] args) throws Exception {
        // 设置 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从数据库读取所有的表，并组装为topic信息，供读取使用 todo database是否多个 从mysql读取这些配置
        String database = YamlUtil.getValueByKey(CONFIG_PATH, "table", "database");
        List<String> owners = YamlUtil.getListByKey(CONFIG_PATH, "table", "owner");
        List<String> topic = new ArrayList<>();

        OracleService oracleTrigger = new OracleService();
        // todo 改为从数据库 获取 db 或者是 owner
        for (int j = 0; j < owners.size(); j++) {
            String owner = owners.get(j);
            List<String> topicNameByDB = oracleTrigger.getTopicNameByDB(database, owner);
            if (CollectionUtils.isNotEmpty(topicNameByDB)) {
                topic.addAll(topicNameByDB);
            }
        }

        String bootstrap = YamlUtil.getValueByKey(CONFIG_PATH, "kafka", "bootstrap");
        for (int i = 0; i < topic.size(); i++) {
            // orcl-dds-t01 => DDS_T01
            String sourceTopic = topic.get(i);
            String[] s = sourceTopic.split("-");
            if (!whiteList.contains(sourceTopic)) {
                continue;
            }

            String sinkTopic = s[1].toUpperCase(Locale.ROOT) + "_" + s[2].toUpperCase(Locale.ROOT);
            // 设置 Kafka 源相关参数
            Properties sourceProps = new Properties();
            sourceProps.setProperty("bootstrap.servers", bootstrap);
            sourceProps.setProperty("group.id", "g1");
            sourceProps.setProperty("scan.startup.mode", "latest-offset");

            // 创建 Kafka 源数据流
            DataStream<String> sourceStream = env.addSource(new FlinkKafkaConsumer<>(
                    sourceTopic,
                    new SimpleStringSchema(),
                    sourceProps
            ));

            // 对每条数据进行反序列化和处理，添加字段name
            DataStream<String> processedStream = sourceStream.map(data -> {
                logger.info("==> get data from kafka [get crud] :{}", data);
                return processData(data);
            });


            // 设置 Kafka 宿相关参数
            Properties sinkProps = new Properties();
            sinkProps.setProperty("bootstrap.servers", bootstrap);

            logger.info("从源topic:{}->宿topic:{}", sourceTopic, sinkTopic);
            // 创建 Kafka 宿数据流
            processedStream.addSink(new FlinkKafkaProducer<>(
                    sinkTopic,
                    new SimpleStringSchema(),
                    sinkProps
            ));
        }
        // 执行程序
        env.execute("开始同步数据作业A!");
    }


    private static String processData(String input) {
        ObjectMapper om = new ObjectMapper();
        try {
            DDSData ddsData = om.readValue(input, DDSData.class);
            return om.writeValueAsString(ddsData.getPayload());
        } catch (IOException e) {
            logger.error("异常情况！", e);
        }
        return "";
    }

}

