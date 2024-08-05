package com.zetyun.hqbank;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;

/**
 * 为热点消息打标并写入kafka，然后存入hbase以及mysql，
 * 用处理完的数据作统计并用来训练模型，大量的高质量的数据会是有用的，
 * 数据源的获取，
 *
 */
public class HotDataPreprocess {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // 1. 数据读取和合并
        DataSet<Tuple2<String, String>> events = readAndMergeData(env, params);

        // 2. 数据去重
        events = events.distinct();

        // 3. NLP处理和重要性过滤
        DataSet<Tuple2<String, String>> importantEvents = filterImportantEvents(events);

        // 输出结果
        if (params.has("output")) {
            importantEvents.writeAsCsv(params.get("output"));
            env.execute("Hot Event Processing");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            importantEvents.print();
        }
    }

    private static DataSet<Tuple2<String, String>> readAndMergeData(ExecutionEnvironment env, MultipleParameterTool params) {
        if (!params.has("input")) {
            System.out.println("Use --input to specify input files");
            return null;
        }

        return env.readCsvFile(params.get("input"))
                .types(String.class, String.class);
    }

    private static DataSet<Tuple2<String, String>> filterImportantEvents(DataSet<Tuple2<String, String>> events) {
        return events.filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> event) throws Exception {
                // 在这里实现NLP处理逻辑
                // 这只是一个简单的示例，您需要根据实际需求实现更复杂的NLP处理
                return !event.f1.toLowerCase().contains("天气很热");
            }
        });
    }
}
