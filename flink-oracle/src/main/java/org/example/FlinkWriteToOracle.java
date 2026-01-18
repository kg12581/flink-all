package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkWriteToOracle {

    public static void main(String[] args) throws Exception {

        // 1️⃣ 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 单并行度，避免 Oracle 写冲突

        // 2️⃣ Table 环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 3️⃣ datagen 源表（使用自增序列，避免主键冲突）
        String sourceDDL =
                "CREATE TABLE src_table ( " +
                        "  id INT, " +
                        "  name STRING, " +
                        "  create_time TIMESTAMP(3), " +
                        "  status INT " +
                        ") WITH ( " +
                        "  'connector' = 'datagen', " +
                        "  'rows-per-second' = '5', " +     // 每秒生成5条
                        "  'fields.id.kind' = 'sequence', " +
                        "  'fields.id.start' = '1', " +
                        "  'fields.id.end' = '100000000', " +
                        "  'fields.name.length' = '6', " + // 随机字符串长度
                        "  'fields.status.min' = '0', " +
                        "  'fields.status.max' = '1' " +
                        ")";

        // 4️⃣ Oracle Sink 表
        String sinkDDL =
                "CREATE TABLE oracle_sink ( " +
                        "  id INT, " +
                        "  name STRING, " +
                        "  create_time TIMESTAMP(3), " +
                        "  status INT, " +
                        "  PRIMARY KEY (id) NOT ENFORCED " +
                        ") WITH ( " +
                        "  'connector' = 'jdbc', " +
                        "  'url' = 'jdbc:oracle:thin:@127.0.0.1:1521/XE', " +
                        "  'table-name' = 'FLINK_SINK_USER', " +
                        "  'username' = 'ogg2', " +
                        "  'password' = 'ogg2', " +
                        "  'driver' = 'oracle.jdbc.OracleDriver', " +
                        "  'sink.buffer-flush.max-rows' = '10', " +
                        "  'sink.buffer-flush.interval' = '1s', " +
                        "  'sink.parallelism' = '1' " +
                        ")";

        // 5️⃣ 执行建表
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // 6️⃣ 数据写入 Oracle（会一直运行）
        tEnv.executeSql(
                "INSERT INTO oracle_sink " +
                        "SELECT id, name, create_time, status FROM src_table"
        );

        System.out.println("Flink 流任务已启动，正在生成数据写入 Oracle...");
    }
}
