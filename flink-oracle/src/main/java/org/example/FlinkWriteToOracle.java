package org.example;



import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
public class FlinkWriteToOracle {

        public static void main(String[] args) throws Exception {

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            EnvironmentSettings settings = EnvironmentSettings
                    .newInstance()
                    .inStreamingMode()
                    .build();

            TableEnvironment tEnv = TableEnvironment.create(settings);

            // 源表（示例数据）
            String sourceDDL =
                    "CREATE TABLE src_table ( " +
                            "  id INT, " +
                            "  name STRING, " +
                            "  create_time TIMESTAMP(3), " +
                            "  status INT " +
                            ") WITH ( " +
                            "  'connector' = 'datagen', " +
                            "  'rows-per-second' = '5' " +
                            ")";

            // Oracle Sink
            String sinkDDL =
                    "CREATE TABLE oracle_sink ( " +
                            "  id INT, " +
                            "  name STRING, " +
                            "  create_time TIMESTAMP(3), " +
                            "  status INT, " +
                            "  PRIMARY KEY (id) NOT ENFORCED " +
                            ") WITH ( " +
                            "  'connector' = 'jdbc', " +
                            "  'url' = 'jdbc:oracle:thin:@127.0.0.1:1521/orclpdb1', " +
                            "  'table-name' = 'FLINK_SINK_USER', " +
                            "  'username' = 'test', " +
                            "  'password' = 'test123', " +
                            "  'driver' = 'oracle.jdbc.OracleDriver', " +
                            "  'sink.buffer-flush.max-rows' = '1000', " +
                            "  'sink.buffer-flush.interval' = '2s' " +
                            ")";

            tEnv.executeSql(sourceDDL);
            tEnv.executeSql(sinkDDL);

            // 写入 Oracle
            tEnv.executeSql(
                    "INSERT INTO oracle_sink SELECT id, name, create_time, status FROM src_table"
            );

        }



}
