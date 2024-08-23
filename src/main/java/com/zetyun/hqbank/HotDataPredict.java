package com.zetyun.hqbank;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zetyun.hqbank.bean.SentenceEntity;
import com.zetyun.hqbank.bean.Student;
import com.zetyun.hqbank.config.StudentDeserializationSchema;
import com.zetyun.hqbank.util.YamlUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.canal.CanalJsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.shaded.org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.hadoop.shaded.org.apache.http.client.methods.HttpPost;
import org.apache.hadoop.shaded.org.apache.http.entity.StringEntity;
import org.apache.hadoop.shaded.org.apache.http.impl.client.CloseableHttpClient;
import org.apache.hadoop.shaded.org.apache.http.impl.client.HttpClients;
import org.apache.hadoop.shaded.org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * 从雪球爬数据并每隔10分钟捞取数据，并调用模型做预测，并将结果写入kafka
 */
public class HotDataPredict {
    private static final Logger logger = LoggerFactory.getLogger(HotDataPredict.class);
    private ObjectMapper objectMapper = new ObjectMapper();
    private static final String TOPIC_PREDICT = "predict_sentence_topic";
    private static final String TOPIC_PREDICT_RESULT = "predict_sentence_result_topic";
    private static final String TOPIC_TRAIN = "train_sentence_topic";
    private static String url = "";

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String userConfigPath = parameters.get("userConfig");
        String systemConfigPath = "D:/conf/windows/systemConfig.yaml";
//        String systemConfigPath = "/opt/flink1.15/config/systemConfig.yaml";

        String bootstrap = YamlUtil.getValueByKey(systemConfigPath, "kafka", "bootstrap");
        String predictUrl = YamlUtil.getValueByKey(systemConfigPath, "service", "predict");
        url = predictUrl;

        logger.info("bootstrap:{}", bootstrap);

        Long checkpointInterval = Long.valueOf(YamlUtil.getValueByKey(userConfigPath, "flink", "checkpointInterval"));


        Configuration flinkConfig = new Configuration();
        flinkConfig.setInteger(RestOptions.PORT, 8888);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
        env.enableCheckpointing(checkpointInterval);

        String sourceTopic = TOPIC_PREDICT;
        String sinkTopic = TOPIC_PREDICT_RESULT;

        Properties sourceProps = new Properties();
        sourceProps.setProperty("group.id", "g1");
        sourceProps.setProperty("scan.startup.mode", "latest-offset");

        // 创建 Kafka 源数据流
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

        // 对每条数据进行反序列化和处理, flink处理 热点信息 步骤
//        DataStream<String> processedStream = sourceStream.map(new PredictionService());
        DataStream<String> processedStream = sourceStream.flatMap(new FlatMapFunction<Student, String>() {
            @Override
            public void flatMap(Student s, Collector<String> collector) throws Exception {
                ObjectMapper objectMapper = new ObjectMapper();
                Student s1 = new Student();
                s1.setAge(1);
                s1.setName("ben");
                s1.setAge(21);

                collector.collect(objectMapper.writeValueAsString(s1));
                Student s2 = new Student();
                s2.setAge(2);
                s2.setName("ben2");
                s2.setAge(212);
                collector.collect(objectMapper.writeValueAsString(s2));
            }

        });

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

    public static class PredictionService implements MapFunction<String, String> {
        private static final long serialVersionUID = 1L;
//        private final String url = "http://127.0.0.1:8008/predict";

        @Override
        public String map(String value) throws Exception {
            logger.info("start now:{}", value);
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                ObjectMapper objectMapper = new ObjectMapper();
                SentenceEntity sentence = objectMapper.readValue(value, SentenceEntity.class);

                HttpPost post = new HttpPost(url);
                post.setHeader("Content-Type", "application/json; charset=UTF-8");

                // 创建请求体
                HashMap<String, Object> map = new HashMap<>();
                map.put("sentence", sentence.getContent());
                map.put("sentence_id", sentence.getSentenceId().toString());

                String json = objectMapper.writeValueAsString(map);

                // 设置请求实体，指定字符编码为 UTF-8
                StringEntity entity = new StringEntity(json, "UTF-8");
                post.setEntity(entity);

                try (CloseableHttpResponse response = httpClient.execute(post)) {
                    if (response.getStatusLine().getStatusCode() == 200) {
                        logger.info("result = {}", response.getEntity().getContent());
                        return EntityUtils.toString(response.getEntity());
                    } else {
                        logger.info("err result = {}", response.getEntity());
                        return "Error: " + response.getStatusLine().getStatusCode();
                    }
                } catch (Exception e) {
                    logger.info("Error:", e);
                    return "Request failed: " + e.getMessage();
                }
            } catch (Exception e) {
                logger.info("Error:", e);
                return "Request failed: " + e.getMessage();
            }
        }
    }

}
