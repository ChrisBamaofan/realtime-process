//package com.zetyun.hqbank;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.data.RowData;
//import org.apache.flink.table.types.DataType;
//import org.apache.flink.table.types.logical.RowType;
//import org.apache.iceberg.flink.TableLoader;
//import org.apache.iceberg.flink.source.FlinkSource;
//
//import java.util.List;
//
///**
// * @author zhaohaojie
// * @date 2024-03-02 17:45
// */
//public class ReadIceberg {
//
//
//    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tabenv = StreamTableEnvironment.create(env);
//        Configuration conf = tabenv.getConfig().getConfiguration();
//        //conf for streaming read
//        conf.setBoolean("table.dynamic-table-options.enabled",true);
//
//        env.enableCheckpointing(30000);
//
//        //1 create catalog
//        tabenv.executeSql("" +
//                "create catalog iceberg_catalog with " +
//                "('type'='iceberg'," +
//                "'catalog-type'='hadoop'," +
//                "'warehouse'='hdfs://172.20.29.5:8020/user/hive/warehouse/'" +
//                ")");
//
//        //2 batch read
//        tabenv.executeSql(" select * from iceberg_catalog.orcl.ice_orcl_dds_string5").print();
//
//        //3 streaming read
////        tabenv.executeSql(" select * from iceberg_catalog.orcl.ice_orcl_dds_string4 /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/ ").print();
//
//    }
//
//}
//
