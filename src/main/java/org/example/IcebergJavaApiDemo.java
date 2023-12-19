package org.example;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.actions.Actions;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IcebergJavaApiDemo {
    private static Logger logger = LoggerFactory.getLogger(IcebergJavaApiDemo.class);

    public static void main(String[] args) throws IOException {
        logger.info("=======");

        String hiveUri = "thrift://172.20.1.34:9083";
        String warehouse = "hdfs://172.20.1.34:8020/user/hive/warehouse/";
        String hive_conf_dir = "/Users/lifenghua/src/iceberg-demo/config/hive-conf-34";
        String hadoop_conf_dir = "/Users/lifenghua/src/iceberg-demo/config/hive-conf-34";



        HiveCatalog catalog = new HiveCatalog();
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        properties.put(CatalogProperties.URI, hiveUri);
        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.hive.HiveCatalog");

        org.apache.hadoop.conf.Configuration cnf = new org.apache.hadoop.conf.Configuration();
        cnf.addResource(new Path(hive_conf_dir, "hive-site.xml"));
        cnf.addResource(new Path(hadoop_conf_dir, "hdfs-site.xml"));
        cnf.addResource(new Path(hadoop_conf_dir, "core-site.xml"));
        catalog.setConf(cnf);

        // 初始化catalog
        catalog.initialize("hive", properties);


//        // 定义表结构schema
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "data", Types.StringType.get()));
//
//        //不进行分区
//        PartitionSpec spec = PartitionSpec.unpartitioned();

        PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).bucket("id", 5, "shared").build();
        Table table;
        // 表的属性
        Map<String, String> properties1 = new HashMap<>();
        properties1.put("engine.hive.enabled", "true");
//
//        // 数据库名,表名
//        TableIdentifier name = TableIdentifier.of("lifh2", "test2");
        TableIdentifier name1 = TableIdentifier.of("default_database", "test_partition_1");
        if (!catalog.tableExists(name1)) {
            System.out.println("table not exists");
            table = catalog.createTable(name1, schema, partitionSpec, properties1);
            table.replaceSortOrder().asc("id", NullOrder.NULLS_LAST).commit();
            table.updateProperties().set("write.upsert.enabled", "true").commit();
        } else {
            System.out.println("table exists");
            table = catalog.loadTable(name1);
        }
//        writeToIceberg(table);
        readFromIceberg(table);
//        rewriteForLittleFile(table);
    }

    private static void rewriteForLittleFile(Table table) {
        Actions.forTable(table).rewriteDataFiles().maxParallelism(5).filter(Expressions.equal("id", 1)).execute();
    }

    private static void readFromIceberg(Table table) {
        IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(table).where(Expressions.equal("id", 1));
//        IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(table);
        CloseableIterable<Record> records = scanBuilder.build();
        long startTime = System.currentTimeMillis();
        System.out.println("startTime: " + startTime);
        for (Record record : records) {
            System.out.println(record);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("endTime: " + endTime);
        System.out.println("diffTime: " + (endTime - startTime));

        TableScan tableScan = table.newScan();

        TableScan filter = tableScan.filter(Expressions.equal("id", 1));

        CloseableIterator<FileScanTask> iterator = filter.planFiles().iterator();

        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }

}