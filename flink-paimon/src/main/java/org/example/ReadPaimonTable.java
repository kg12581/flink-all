package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ReadPaimonTable {

    public static void main(String[] args) throws Exception {
        // 创建流执行环境（也可以用 Batch，但 Stream 通用）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. 注册 Paimon Catalog（必须和写入时的 warehouse 一致）
        tEnv.executeSql(
                "CREATE CATALOG paimon_catalog WITH (" +
                        "  'type' = 'paimon'," +
                        "  'warehouse' = 'hdfs://localhost:8020/user/paimon'" +
                        ")"
        );

        // 2. 使用该 catalog
        tEnv.executeSql("USE CATALOG paimon_catalog");
        tEnv.executeSql("USE ods"); // 切换到数据库

        // 3. 查询并打印数据
        System.out.println("Reading data from Paimon table: ods.paimon_courses");
//        tEnv.executeSql("SELECT * FROM paimon_courses")
//                .print(); // 触发执行并打印到控制台


        tEnv.executeSql(
                "SELECT * FROM paimon_catalog.ods.paimon_courses " +
                        "/*+ OPTIONS('streaming'='true', 'monitor-interval'='2s') */"
        ).print();
    }
}
