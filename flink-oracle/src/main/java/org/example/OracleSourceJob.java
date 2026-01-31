package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.bean.CdcTable;
import org.example.OracleJdbcSource;

public class OracleSourceJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(4); // 并行度 = 分片数

        String url = "jdbc:oracle:thin:@127.0.0.1:1521/XE";
        String user = "ogg2";
        String pwd = "ogg2";

        // 假设表名为 TEST_CDC_TABLE，主键为 ID
        OracleJdbcSource source = new OracleJdbcSource(
                url, user, pwd,
                "TEST_CDC_TABLE",
                "ID",          // 主键
                5000,          // 每批 5000 行
                true           // true=12c+, false=11g
        );

        env.addSource(source)
                .print();

        env.execute("Custom Oracle Parallel Source");
    }
}