#flink-cdc
该项目主要将 DDS 服务输出结果转为 iceberg表供 impala实时查询，此项目分两个作业，作业A功能为
将DDS的输出结果，例如 oracle数据库的 orcl库 DDS 用户 T01表 的变更数据 update、insert、delete的信息
写入 topic: orcl-dds-t01，然后作业A从此topic读取出数据，并把结果转为debezium的格式，即去除schema
只保留payload，并写入kafka topic:DDS-T01

作业B功能为 将 topic:DDS-T01的数据映射为一张 flink table，再创建一张 iceberg的表，将数据从前表实时
同步到后表中。实现变更流式处理。

