package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaDebeziumToPaimon {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // ⭐⭐⭐ 关键：开启 checkpoint ⭐⭐⭐
        env.enableCheckpointing(10000); // 每 10 秒一次
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. 创建 Paimon Catalog
        tEnv.executeSql(
                "CREATE CATALOG paimon_catalog WITH (" +
                        "  'type' = 'paimon'," +
                        "  'warehouse' = 'hdfs://localhost:8020/user/paimon'" +
                        ")"
        );

        tEnv.executeSql("USE CATALOG paimon_catalog");
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS ods");

        tEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS ods.paimon_courses (" +
                        "  course_id INT," +
                        "  course_name STRING," +
                        "  teacher_id INT," +
                        "  category STRING," +
                        "  difficulty STRING," +
                        "  PRIMARY KEY (course_id) NOT ENFORCED" +
                        ") WITH (" +
                        "  'bucket' = '2'," +
                        "  'changelog-producer' = 'input'" +
                        ")"
        );

        // 切回 default catalog 创建 Kafka 表
        tEnv.executeSql("USE CATALOG default_catalog");
        tEnv.executeSql("USE default_database");

        tEnv.executeSql(
                "CREATE TEMPORARY TABLE kafka_courses (" +
                        "  course_id INT," +
                        "  course_name STRING," +
                        "  teacher_id INT," +
                        "  category STRING," +
                        "  difficulty STRING," +
                        "  PRIMARY KEY (course_id) NOT ENFORCED" +
                        ") WITH (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = 'courses'," +
                        "  'properties.bootstrap.servers' = 'localhost:9092'," +
                        "  'properties.group.id' = 'kafka2paimon-group'," +
                        "  'scan.startup.mode' = 'earliest-offset'," +
                        "  'format' = 'debezium-json'" +
                        ")"
        );

        // ✅ 新增：创建 PRINT 表用于控制台输出
//        tEnv.executeSql(
//                "CREATE TEMPORARY TABLE print_table (" +
//                        "  course_id INT," +
//                        "  course_name STRING," +
//                        "  teacher_id INT," +
//                        "  category STRING," +
//                        "  difficulty STRING" +
//                        ") WITH (" +
//                        "  'connector' = 'print'" +
//                        ")"
//        );

        // ✅ 新增：启动一个打印任务（异步）
//        tEnv.executeSql("INSERT INTO print_table SELECT * FROM kafka_courses");

        // 主任务：写入 Paimon
        tEnv.executeSql(
                "INSERT INTO paimon_catalog.ods.paimon_courses " +
                        "SELECT course_id, course_name, teacher_id, category, difficulty " +
                        "FROM kafka_courses"
        ).await();
    }
}