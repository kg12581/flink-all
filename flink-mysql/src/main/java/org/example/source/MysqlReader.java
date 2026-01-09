package org.example.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public class MysqlReader {

    public static void main(String[] args) throws Exception {

        // 1. StreamExecutionEnvironment
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. EnvironmentSettings（Streaming）
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        // ✅ 只创建 StreamTableEnvironment
        StreamTableEnvironment tableEnv =
                StreamTableEnvironment.create(env, settings);

        // 3. 创建 MySQL JDBC 表
        String mysqlTableSql =
                "CREATE TABLE mysql_user ( " +
                        "  id INT, " +
                        "  difficulty INT, " +
                        "  name STRING " +
                        ") WITH ( " +
                        "  'connector' = 'jdbc', " +
                        "  'url' = 'jdbc:mysql://localhost:3306/qianfeng?useSSL=false', " +
                        "  'table-name' = 'question_difficulty', " +
                        "  'username' = 'admin', " +
                        "  'password' = 'Admin@123456' " +
                        ")";

        tableEnv.executeSql(mysqlTableSql);

        // 4. SQL 查询
        Table resultTable =
                tableEnv.sqlQuery("SELECT id, difficulty, name FROM mysql_user");

        // 5. Table → DataStream（JDBC 是 Append-only，OK）
        DataStream<Row> resultStream =
                tableEnv.toDataStream(resultTable);

        // 6. 打印
        resultStream.print();

        // 7. 执行
        env.execute("Flink SQL MySQL to DataStream Print");
    }
}
