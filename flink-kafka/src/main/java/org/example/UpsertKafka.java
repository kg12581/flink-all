package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UpsertKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // === Step 1: CDC 源表（courses）===
        tEnv.executeSql(
                "CREATE TABLE teachers_cdc (" +
                        "  teacher_id INT," +
                        "  teacher_name STRING," +
                        "  hire_date date," +
                        "  department STRING," +
                        "  PRIMARY KEY (teacher_id) NOT ENFORCED" +
                        ") WITH (" +
                        "  'connector' = 'mysql-cdc'," +
                        "  'hostname' = 'localhost'," +
                        "  'port' = '3306'," +
                        "  'username' = 'root'," +
                        "  'password' = 'Admin@123456'," +
                        "  'database-name' = 'qianfeng'," +
                        "  'table-name' = 'teachers'" +
                        ")"
        );

        // === Step 2: Upsert Kafka Sink 表（注意：主键也用 NOT ENFORCED！）===
        tEnv.executeSql(
                "CREATE TABLE teachers_upsert_kafka (" +
                        "  teacher_id INT," +
                        "  teacher_name STRING," +
                        "  hire_date date," +
                        "  department STRING," +
                        "  PRIMARY KEY (teacher_id) NOT ENFORCED" +  // ← 关键：必须是 NOT ENFORCED
                        ") WITH (" +
                        "  'connector' = 'upsert-kafka'," +
                        "  'topic' = 'teachers-upsert'," +
                        "  'properties.bootstrap.servers' = 'localhost:9092'," +
                        "  'key.format' = 'json'," +
                        "  'value.format' = 'json'" +
                        ")"
        );

        // === Step 3: 同步数据 ===
        tEnv.executeSql(
                "INSERT INTO teachers_upsert_kafka " +
                        "SELECT teacher_id, teacher_name, hire_date, department " +
                        "FROM teachers_cdc"
        );

//        env.execute("Flink CDC to Upsert Kafka - Courses");
    }
}