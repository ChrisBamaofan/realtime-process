package com.zetyun.hqbank;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zetyun.hqbank.bean.dds.DDSData;
import com.zetyun.hqbank.bean.dds.DDSPayload;
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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.zetyun.hqbank.bean.dds.DDSPayload.encrypt;


/**
 * flink datastream 将数据从 topic1 发送到 topic2,中间将数据清洗
 *
 * @author zhaohaojie
 * @date 2023-12-20 10:32
 */
public class DDS2FlinkCDC {
    private static final Logger logger = LoggerFactory.getLogger(DDS2FlinkCDC.class);

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String userConfigPath = parameters.get("userConfig");
        String systemConfigPath = "/opt/flink-on-yarn/conf/systemConfig.yaml";

        // 设置 Flink 环境
        String jaasConf = YamlUtil.getValueByKey(systemConfigPath, "kerberos", "jaasConf");
        String krb5Conf = YamlUtil.getValueByKey(systemConfigPath, "kerberos", "krb5Conf");
        String krb5Keytab = YamlUtil.getValueByKey(systemConfigPath, "kerberos", "krb5Keytab");
        String principal = YamlUtil.getValueByKey(systemConfigPath, "kerberos", "principal");
        String bootstrap = YamlUtil.getValueByKey(systemConfigPath, "kafka", "bootstrap");
        // db config
        String oracleUri = YamlUtil.getValueByKey(userConfigPath, "oracle", "url");
        oracleUri = Jasypt.decrypt(oracleUri);
        String[] parts = oracleUri.split("/");
        String database = parts[1];
        List<String> owners = YamlUtil.getListByKey(userConfigPath, "table", "owner");
        List<String> tables = YamlUtil.getListByKey(userConfigPath, "table", "tableNames");
        List<String> whiteList = KafkaUtil.getKafkaTopic(database,owners.get(0),tables);

        logger.info("jaasConf:{}", jaasConf);
        logger.info("krb5Conf:{}", krb5Conf);
        logger.info("krb5Keytab:{}", krb5Keytab);
        logger.info("principal:{}", principal);
        logger.info("bootstrap:{}", bootstrap);
        logger.info("database:{}", database);
        logger.info("owners:{}", owners);
        logger.info("whiteList:{}", whiteList);

        // flink 指定 jaas 必须此配置 用于认证
        System.setProperty("java.security.auth.login.config", jaasConf);
        System.setProperty("java.security.krb5.conf", krb5Conf);

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
        // 从指定的库以及 owner下 拼接出 所有的 dds写入的 topic名
        for (String owner : owners) {
            List<String> topicNameByDB = oracleTrigger.getTopicNameByDB(userConfigPath,database, owner);
            if (CollectionUtils.isNotEmpty(topicNameByDB)) {
                topic.addAll(topicNameByDB);
            }
        }

        // 从指定的库以及 owner下 拼接出 所有的 dds写入的 topic名,orcl-dds-t_zhj2
        for (String sourceTopic : topic) {
            if (CollectionUtils.isNotEmpty(whiteList)) {
                if (!whiteList.contains(sourceTopic)) {
                    continue;
                }
            }
            // target-orcl-dds-t_zhj2
            String sinkTopic = "target-"+sourceTopic;
            // 设置 Kafka 源相关参数
            Properties sourceProps = new Properties();
            sourceProps.setProperty("group.id", "g1");
            sourceProps.setProperty("scan.startup.mode", "latest-offset");
            sourceProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            sourceProps.setProperty(SaslConfigs.SASL_MECHANISM, "GSSAPI");
            sourceProps.setProperty(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");

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
                logger.debug("==> get data from kafka [get crud] :{}", data);
                return processData(data,"test");
            }).filter(new FilterFunction<String>() {
                @Override
                public boolean filter(String value) throws Exception {
                    ObjectMapper om = new ObjectMapper();
                    DDSPayload ddsPayload = om.readValue(value, DDSPayload.class);
                    return !"ddl".equals(ddsPayload.getOp());
                }
            });


            // 设置 Kafka 宿相关参数
            Properties sinkProps = new Properties();
            sinkProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            sinkProps.setProperty(SaslConfigs.SASL_MECHANISM, "GSSAPI");
            sinkProps.setProperty(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");

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
                    .setKafkaProducerConfig(sinkProps)
                    .build();
            processedStream.sinkTo(sink);
        }
        // 执行程序
        env.execute("同步数据作业A："+ whiteList);

    }

    /**
     * {"scn":88125961,"tms":"2024-02-06 10:18:06","xid":"11.7.8102","payload":{"op":"c","schema":{"owner":"DDS","table":"TEST2"},"row":1,"rid":"AAAZUBAAEAAIllEAAG","after":{"CHAR1":"3","CHAR2":"3","CHAR3":"3tt","DATE_1":"2024-03-03 00:00:00","CHAR4":null,"REAL1":null,"NUM1":null,"NUM2":null,"INT1":null,"FLOAT1":null}}}
     * {"CHAR1":"3","CHAR2":"3","CHAR3":"3tt","DATE_1":"2024-03-03 00:00:00","CHAR4":null,"REAL1":null,"NUM1":null,"NUM2":null,"INT1":null,"FLOAT1":null}
     *
     * {"scn":88127297,"tms":"2024-02-06 10:39:12","xid":"11.15.8101","payload":{"op":"u","schema":{"owner":"DDS","table":"TEST2"},"row":1,"rid":"AAAZUBAAEAAIllEAAG","before":{"CHAR1":"3","CHAR2":"3","CHAR3":"3tt","DATE_1":"2024-03-03 00:00:00","CHAR4":null,"REAL1":null,"NUM1":null,"NUM2":null,"INT1":null,"FLOAT1":null},"after":{"CHAR1":"4","CHAR2":"3","CHAR3":"3tt","DATE_1":"2024-03-03 00:00:00","CHAR4":null,"REAL1":null,"NUM1":null,"NUM2":null,"INT1":null,"FLOAT1":null}}}
     * {"CHAR1":"3","CHAR2":"3","CHAR3":"3tt","DATE_1":"2024-03-03 00:00:00","CHAR4":null,"REAL1":null,"NUM1":null,"NUM2":null,"INT1":null,"FLOAT1":null}
     * {"CHAR1":"4","CHAR2":"3","CHAR3":"3tt","DATE_1":"2024-03-03 00:00:00","CHAR4":null,"REAL1":null,"NUM1":null,"NUM2":null,"INT1":null,"FLOAT1":null}
     *
     * */
    public static String processData(String input, String schema) {
        ObjectMapper om = new ObjectMapper();
        DDSPayload p = new DDSPayload();
        DDSPayload dummy = p.createDummy(schema);
        if (StringUtils.isEmpty(input)) {
            try {
                return om.writeValueAsString(dummy);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
        try {
            DDSData ddsData = om.readValue(input, DDSData.class);
            // 如果新增 op = c，全量更新 op = c，则为每一行增加一列，auto_md5_id = DDSData.payload 的 md5
            //
            DDSPayload payload = ddsData.getPayload();
            if(StringUtils.equalsIgnoreCase(DDSOprEnums.INSERT.getOperateName(), payload.getOp())){
                LinkedHashMap<String,Object> after_map = (LinkedHashMap) payload.getAfter();
                String afterStr = after_map.toString();
                try {
                    String auto_md5_id = encrypt(afterStr);
                    after_map.put("auto_md5_id",auto_md5_id);
                    payload.setAfter(after_map);
                    logger.debug("<== construct data : {}",after_map);
                    return om.writeValueAsString(payload);
                } catch (Exception e) {
                    logger.error("！！！error when assembling dds data!!!",e);
                }
            }else if (StringUtils.equalsIgnoreCase(DDSOprEnums.UPDATE.getOperateName(),payload.getOp())){
                LinkedHashMap<String,Object> after_map = (LinkedHashMap) payload.getAfter();
                LinkedHashMap<String,Object> before_map = (LinkedHashMap) payload.getBefore();
                String afterStr = after_map.toString();
                String beforeStr = before_map.toString();
                try {
                    String after_auto_md5_id = encrypt(afterStr);
                    after_map.put("auto_md5_id",after_auto_md5_id);
                    payload.setAfter(after_map);
                    logger.debug("<== construct data : {}",after_map);

                    String before_auto_md5_id = encrypt(beforeStr);
                    before_map.put("auto_md5_id",before_auto_md5_id);
                    payload.setBefore(before_map);
                    logger.debug("<== construct data : {}",before_map);
                    return om.writeValueAsString(payload);
                } catch (Exception e) {
                    logger.error("！！！error when assembling dds data!!!",e);
                }
            }else if (StringUtils.equalsIgnoreCase(DDSOprEnums.DELETE.getOperateName(),payload.getOp())){
                LinkedHashMap<String,Object> before_map = (LinkedHashMap) payload.getBefore();
                String beforeStr = before_map.toString();
                try {
                    String auto_md5_id = encrypt(beforeStr);
                    before_map.put("auto_md5_id",auto_md5_id);
                    payload.setBefore(before_map);
                    logger.info("<== construct data : {}",before_map);
                    return om.writeValueAsString(payload);
                } catch (Exception e) {
                    logger.error("！！！error when assembling dds data!!!",e);
                }
            }

            return om.writeValueAsString(ddsData.getPayload());
        } catch (IOException e) {
            logger.error("[processData] 异常情况！", e);
        }
        try {
            return om.writeValueAsString(dummy);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }
}