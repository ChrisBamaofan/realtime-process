//package com.zetyun.hqbank;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.zetyun.hqbank.util.YamlUtil;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
//import org.apache.hadoop.shaded.org.apache.http.client.methods.CloseableHttpResponse;
//import org.apache.hadoop.shaded.org.apache.http.client.methods.HttpPost;
//import org.apache.hadoop.shaded.org.apache.http.entity.StringEntity;
//import org.apache.hadoop.shaded.org.apache.http.impl.client.CloseableHttpClient;
//import org.apache.hadoop.shaded.org.apache.http.impl.client.HttpClients;
//import org.apache.hadoop.shaded.org.apache.http.util.EntityUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.Properties;
//
///**
// * 为热点消息打标并写入kafka，然后存入hbase以及mysql，
// * 用处理完的数据作统计并用来训练模型，大量的高质量的数据会是有用的，
// * 数据源的获取，
// *
// */
//public class HotDataForkTag {
//    private static final Logger logger = LoggerFactory.getLogger(DDS2FlinkCDCSimple.class);
//    private ObjectMapper objectMapper = new ObjectMapper();
//    private static final String TOPIC_PREDICT ="predict_sentence_topic";
//    private static final String TOPIC_TRAIN ="train_sentence_topic";
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
//        //todo checkpoint 实现原理，工作效果
//        Long checkpointInterval = Long.valueOf(YamlUtil.getValueByKey(userConfigPath, "flink", "checkpointInterval"));
//
//        Configuration flinkConfig = new Configuration();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
//        env.enableCheckpointing(checkpointInterval);
//
//
//        // 设置 Kafka 源相关参数
//        Properties sourceProps = new Properties();
//        sourceProps.setProperty("group.id", "g1");
//        sourceProps.setProperty("scan.startup.mode", "latest-offset");
//
//        // 创建 Kafka 源数据流
//        KafkaSource<String> source = KafkaSource.<String>builder()
//                .setBootstrapServers(bootstrap)
//                .setTopics(TOPIC_TRAIN)
//                .setGroupId("g1")
//                .setStartingOffsets(OffsetsInitializer.latest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .setProperties(sourceProps)
//                .build();
//
//        DataStreamSource<String> sourceStream =
//                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source" + sourceTopic);
//
//        // 对每条数据进行反序列化和处理, flink处理 热点信息 步骤
//        DataStream<String> processedStream = sourceStream.map(new PredictionService());
//        // 设置 Kafka 宿相关参数
//        logger.info("==> 从源topic:{}->宿topic:{}", TOPIC_TRAIN, sinkTopic);
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
//        processedStream.sinkTo(sink);
//        env.execute("同步数据作业A：");
//    }
//
//    public static class PredictionService implements MapFunction<String, String> {
//        private static final long serialVersionUID = 1L;
//        private final String url = "http://127.0.0.1:8088/predict";
//
//        @Override
//        public String map(String value) throws Exception {
//            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
//                HttpPost post = new HttpPost(url);
//                post.setEntity(new StringEntity(value));
//
//                try (CloseableHttpResponse response = httpClient.execute(post)) {
//                    if (response.getStatusLine().getStatusCode() == 200) {
//                        return EntityUtils.toString(response.getEntity());
//                    } else {
//                        return "Error: " + response.getStatusLine().getStatusCode();
//                    }
//                }
//            } catch (IOException e) {
//                return "Request failed: " + e.getMessage();
//            }
//        }
//    }
//}
