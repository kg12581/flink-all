package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.jdbc.JdbcInputFormat; // 核心类（替代JdbcSource）
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.example.bean.CdcTable;

public class FlinkReadFromOracle {

        public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);

            EnvironmentSettings settings = EnvironmentSettings.newInstance()
//                    .inStreamingMode()
                    .build();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


            String tableSql = "CREATE TABLE TEST_CDC_TABLE (\n" +
                    "    ID        INT,\n" +
                    "    NAME STRING,\n" +
                    "    CREATE_TIME    TIMESTAMP(6),\n" +
//                    "    STATUS    DECIMAL(10, 2),\n" +
                    "    STATUS INT\n" +
                    ") WITH (\n" +
                    "    'connector' = 'jdbc',\n" +
                    "    'url' = 'jdbc:oracle:thin:@127.0.0.1:1521/XE',\n" +
                    "    'table-name' = 'TEST_CDC_TABLE',\n" +
                    "    'username' = 'ogg2',\n" +
                    "    'password' = 'ogg2',\n" +
                    "    'driver' = 'oracle.jdbc.OracleDriver'\n" +
                    ");\n";
            tableEnv.executeSql(tableSql);
            Table table = tableEnv.sqlQuery("select ID,NAME,CREATE_TIME,STATUS from TEST_CDC_TABLE");
            DataStream<CdcTable> dataStream = tableEnv.toDataStream(table, CdcTable.class);
            dataStream.print();
            env.execute();


        }

}


