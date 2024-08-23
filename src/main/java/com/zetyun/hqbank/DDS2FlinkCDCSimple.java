//package com.zetyun.hqbank;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.zetyun.hqbank.bean.dds.DDSData;
//import com.zetyun.hqbank.bean.dds.DDSPayload;
//import com.zetyun.hqbank.enums.DDSOprEnums;
//import com.zetyun.hqbank.util.YamlUtil;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.state.MapState;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.base.DeliveryGuarantee;
//import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
//import org.apache.flink.connector.kafka.sink.KafkaSink;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.ProcessFunction;
//import org.apache.flink.util.Collector;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.LinkedHashMap;
//import java.util.Properties;
//
//import static com.zetyun.hqbank.bean.dds.DDSPayload.encrypt;
//
//
///**
// * flink datastream 将数据从 topic1 发送到 topic2,中间将数据清洗
// *
// * @author zhaohaojie
// * @date 2023-12-20 10:32
// */
//public class DDS2FlinkCDCSimple {
//    private static final Logger logger = LoggerFactory.getLogger(DDS2FlinkCDCSimple.class);
//    private ObjectMapper objectMapper = new ObjectMapper();
//
//    public static void main(String[] args) throws Exception {
//        ParameterTool parameters = ParameterTool.fromArgs(args);
//        String userConfigPath = parameters.get("userConfig");
//        String systemConfigPath = "D:/conf/windows/systemConfig.yaml";
//
//        String bootstrap = YamlUtil.getValueByKey(systemConfigPath, "kafka", "bootstrap");
//
//        logger.info("bootstrap:{}", bootstrap);
//
//        Long checkpointInterval = Long.valueOf(YamlUtil.getValueByKey(userConfigPath, "flink", "checkpointInterval"));
//
//        Configuration flinkConfig = new Configuration();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
//        env.enableCheckpointing(checkpointInterval);
//
//
//        String sourceTopic = "source-topic";
//        String sinkTopic = "target-topic";
//        // 设置 Kafka 源相关参数
//        Properties sourceProps = new Properties();
//        sourceProps.setProperty("group.id", "g1");
//        sourceProps.setProperty("scan.startup.mode", "latest-offset");
////        sourceProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
////        sourceProps.setProperty(SaslConfigs.SASL_MECHANISM, "GSSAPI");
////        sourceProps.setProperty(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
//
//        // 创建 Kafka 源数据流
//        KafkaSource<String> source = KafkaSource.<String>builder()
//                .setBootstrapServers(bootstrap)
//                .setTopics(sourceTopic)
//                .setGroupId("g1")
//                .setStartingOffsets(OffsetsInitializer.latest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .setProperties(sourceProps)
//                .build();
//
//        DataStreamSource<String> sourceStream =
//                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source" + sourceTopic);
//
//        // 对每条数据进行反序列化和处理
//        DataStream<String> processedStream = sourceStream.map(data -> {
//            logger.info("==> get data from kafka [get crud] :{}", data);
//            return processData(data, "test");
//        }).filter(new FilterFunction<String>() {
//            @Override
//            public boolean filter(String value) throws Exception {
//                ObjectMapper om = new ObjectMapper();
//                DDSPayload ddsPayload = om.readValue(value, DDSPayload.class);
//                return !"ddl".equals(ddsPayload.getOp());
//            }
//        });
//        // 二次处理，对upsert的数据拆分为两条
//        DataStream<String> splittedDataStream = processedStream.keyBy(new KeySelector<String, String>() {
//            @Override
//            public String getKey(String value) throws Exception {
//                ObjectMapper objectMapper = new ObjectMapper();
//                // Parse your Debezium data here and return the key
//                // For example, if your Debezium data is JSON, you can use a JSON parser
//                // to extract the necessary information
//                DDSPayload ddsPayload = objectMapper.readValue(value, DDSPayload.class);
//                // Assuming the key is stored in the 'id' field
//                return ddsPayload.getOp(); // Return the key for partitioning
//            }
//        }).process(new SplitProcessFunction());
//
//        // 设置 Kafka 宿相关参数
//        logger.info("==> 从源topic:{}->宿topic:{}", sourceTopic, sinkTopic);
//        // 创建 Kafka 宿数据流
//        KafkaSink<String> sink = KafkaSink.<String>builder()
//                .setBootstrapServers(bootstrap)
//                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//                        .setTopic(sinkTopic)
//                        .setValueSerializationSchema(new SimpleStringSchema())
//                        .build()
//                )
//                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
//                .build();
//        splittedDataStream.sinkTo(sink);
//        env.execute("同步数据作业A：");
//    }
//
//    public static class SplitProcessFunction extends ProcessFunction<String, String> {
//
//        private transient MapState<String, Boolean> state;
//        private ObjectMapper objectMapper = new ObjectMapper();
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            super.open(parameters);
//            MapStateDescriptor<String, Boolean> descriptor =
//                    new MapStateDescriptor<>("splitState", String.class, Boolean.class);
//            state = getRuntimeContext().getMapState(descriptor);
//        }
//
//        @Override
//        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
//            // Parse your Debezium data here
//            // For example, if your Debezium data is JSON, you can use a JSON parser
//            // to extract the necessary information
//            DDSPayload ddsPayload = objectMapper.readValue(value, DDSPayload.class);
//            String op = ddsPayload.getOp();
//            // if update then change to insert and delete
//            if (!op.equals("u")) {
//                // Emit new data
//                out.collect(value);
//            } else {
//                // Emit old data
//                DDSPayload create = createCreate(ddsPayload);
//                DDSPayload delete = createDelete(ddsPayload);
//                String createString = objectMapper.writeValueAsString(create);
//                String deleteString = objectMapper.writeValueAsString(delete);
//                out.collect(createString);
//                out.collect(deleteString);
//            }
//        }
//
//        private DDSPayload createDelete(DDSPayload ddsPayload) {
//            DDSPayload deleteObj = new DDSPayload();
//            deleteObj.setOp("d");
//            deleteObj.setRow(ddsPayload.getRow());
//            deleteObj.setSchema(ddsPayload.getSchema());
//            deleteObj.setBefore(ddsPayload.getBefore());
//
//            return deleteObj;
//        }
//
//        private DDSPayload createCreate(DDSPayload ddsPayload) {
//            DDSPayload createObj = new DDSPayload();
//            createObj.setOp("c");
//            createObj.setRow(ddsPayload.getRow());
//            createObj.setSchema(ddsPayload.getSchema());
//            createObj.setAfter(ddsPayload.getAfter());
//            return createObj;
//        }
//    }
//
//
//    public static String processData(String input, String schema) {
//        ObjectMapper om = new ObjectMapper();
//        DDSPayload p = new DDSPayload();
//        DDSPayload dummy = p.createDummy(schema);
//        if (StringUtils.isEmpty(input)) {
//            try {
//                return om.writeValueAsString(dummy);
//            } catch (JsonProcessingException e) {
//                e.printStackTrace();
//            }
//        }
//        try {
//            DDSData ddsData = om.readValue(input, DDSData.class);
//            // 如果新增 op = c，全量更新 op = c，则为每一行增加一列，auto_md5_id = DDSData.payload 的 md5
//            //
//            DDSPayload payload = ddsData.getPayload();
//            if (StringUtils.equalsIgnoreCase(DDSOprEnums.INSERT.getOperateName(), payload.getOp())) {
//                LinkedHashMap<String, Object> after_map = (LinkedHashMap) payload.getAfter();
//                LinkedHashMap<String, Object> newAfter_map = new LinkedHashMap<>();
//                String afterStr = after_map.toString();
//                try {
//                    String auto_md5_id = encrypt(afterStr);
//                    newAfter_map.put("AUTO_MD5_ID", auto_md5_id);
//                    newAfter_map.putAll(after_map);
//                    payload.setAfter(newAfter_map);
//                    logger.info("<== construct data : {}", payload);
//                    return om.writeValueAsString(payload);
//                } catch (Exception e) {
//                    logger.error("！！！error when assembling dds data!!!", e);
//                }
//            } else if (StringUtils.equalsIgnoreCase(DDSOprEnums.UPDATE.getOperateName(), payload.getOp())) {
//                LinkedHashMap<String, Object> after_map = (LinkedHashMap) payload.getAfter();
//                LinkedHashMap<String, Object> before_map = (LinkedHashMap) payload.getBefore();
//                LinkedHashMap<String, Object> newAfter_map = new LinkedHashMap<>();
//                LinkedHashMap<String, Object> newBefore_map = new LinkedHashMap<>();
//
//                String afterStr = after_map.toString();
//                String beforeStr = before_map.toString();
//                try {
//                    String after_auto_md5_id = encrypt(afterStr);
//                    newAfter_map.put("AUTO_MD5_ID", after_auto_md5_id);
//                    newAfter_map.putAll(after_map);
//
//                    payload.setAfter(newAfter_map);
//                    logger.info("<== construct data : {}", payload);
//
//                    String before_auto_md5_id = encrypt(beforeStr);
//                    newBefore_map.put("AUTO_MD5_ID", before_auto_md5_id);
//                    newBefore_map.putAll(before_map);
//                    payload.setBefore(newBefore_map);
//                    logger.info("<== construct data : {}", payload);
//                    return om.writeValueAsString(payload);
//                } catch (Exception e) {
//                    logger.error("！！！error when assembling dds data!!!", e);
//                }
//            } else if (StringUtils.equalsIgnoreCase(DDSOprEnums.DELETE.getOperateName(), payload.getOp())) {
//                LinkedHashMap<String, Object> before_map = (LinkedHashMap) payload.getBefore();
//                String beforeStr = before_map.toString();
//                LinkedHashMap<String, Object> newBefore_map = new LinkedHashMap<>();
//                try {
//                    String auto_md5_id = encrypt(beforeStr);
//                    newBefore_map.put("AUTO_MD5_ID", auto_md5_id);
//                    newBefore_map.putAll(before_map);
//                    payload.setBefore(newBefore_map);
//                    logger.info("<== construct data : {}", payload);
//                    return om.writeValueAsString(payload);
//                } catch (Exception e) {
//                    logger.error("！！！error when assembling dds data!!!", e);
//                }
//            }
//
//            return om.writeValueAsString(ddsData.getPayload());
//        } catch (IOException e) {
//            logger.error("[processData] 异常情况！", e);
//        }
//        try {
//            return om.writeValueAsString(dummy);
//        } catch (JsonProcessingException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
//}