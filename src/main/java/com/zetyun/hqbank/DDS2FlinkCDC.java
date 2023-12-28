package com.zetyun.hqbank;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zetyun.hqbank.bean.dds.DDSData;
import com.zetyun.hqbank.service.oracle.OracleService;
import com.zetyun.hqbank.util.YamlUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
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

    public static void main(String[] args) throws Exception {
        // 设置 Flink 环境
        String jaasConf = YamlUtil.getValueByKey(CONFIG_PATH, "kerberos", "jaasConf");
        String krb5Conf = YamlUtil.getValueByKey(CONFIG_PATH, "kerberos", "krb5Conf");
        String krb5Keytab = YamlUtil.getValueByKey(CONFIG_PATH, "kerberos", "krb5Keytab");
        String principal = YamlUtil.getValueByKey(CONFIG_PATH, "kerberos", "principal");
        // 数据库配置
        String bootstrap = YamlUtil.getValueByKey(CONFIG_PATH, "kafka", "bootstrap");
        String database = YamlUtil.getValueByKey(CONFIG_PATH, "table", "database");
        List<String> owners = YamlUtil.getListByKey(CONFIG_PATH, "table", "owner");
        // 白名单配置
        List<String> whiteList = YamlUtil.getListByKey(CONFIG_PATH, "table", "whiteListA");

        // flink 指定 jaas 必须此配置 用于认证
        System.setProperty("java.security.auth.login.config", jaasConf);

        Properties flinkProps = new Properties();
        flinkProps.setProperty("security.kerberos.krb5-conf.path", krb5Conf);
        flinkProps.setProperty("security.kerberos.login.keytab", krb5Keytab);
        flinkProps.setProperty("security.kerberos.login.principal", principal);
        flinkProps.setProperty("security.kerberos.login.contexts", "Client,KafkaClient");
        flinkProps.setProperty("state.backend", "hashmap");

        Configuration flinkConfig = new Configuration();
        flinkConfig.addAllToProperties(flinkProps);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);

        List<String> topic = new ArrayList<>();
        OracleService oracleTrigger = new OracleService();

        for (String owner : owners) {
            List<String> topicNameByDB = oracleTrigger.getTopicNameByDB(database, owner);
            if (CollectionUtils.isNotEmpty(topicNameByDB)) {
                topic.addAll(topicNameByDB);
            }
        }

        for (String sourceTopic : topic) {
            String[] s = sourceTopic.split("-");
            if (CollectionUtils.isNotEmpty(whiteList)) {
                if (!whiteList.contains(sourceTopic)) {
                    continue;
                }
            }

            String sinkTopic = s[1].toUpperCase(Locale.ROOT) + "_" + s[2].toUpperCase(Locale.ROOT);
            // 设置 Kafka 源相关参数
            Properties sourceProps = new Properties();
            sourceProps.setProperty("bootstrap.servers", bootstrap);
            sourceProps.setProperty("group.id", "g1");
            sourceProps.setProperty("scan.startup.mode", "latest-offset");
            sourceProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            sourceProps.setProperty(SaslConfigs.SASL_MECHANISM, "GSSAPI");
            sourceProps.setProperty(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");

            // 创建 Kafka 源数据流
            DataStream<String> sourceStream = env.addSource(new FlinkKafkaConsumer<>(
                    sourceTopic,
                    new SimpleStringSchema(),
                    sourceProps
            ));

            // 对每条数据进行反序列化和处理
            DataStream<String> processedStream = sourceStream.map(data -> {
                logger.info("==> get data from kafka [get crud] :{}", data);
                return processData(data);
            });


            // 设置 Kafka 宿相关参数
            Properties sinkProps = new Properties();
            sinkProps.setProperty("bootstrap.servers", bootstrap);
            sinkProps.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            sinkProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            sinkProps.setProperty(SaslConfigs.SASL_MECHANISM, "GSSAPI");
            sinkProps.setProperty(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");

            logger.info("从源topic:{}->宿topic:{}", sourceTopic, sinkTopic);
            // 创建 Kafka 宿数据流
//            processedStream.print();
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
            logger.error("[processData] 异常情况！", e);
        }
        return "";
    }

}

