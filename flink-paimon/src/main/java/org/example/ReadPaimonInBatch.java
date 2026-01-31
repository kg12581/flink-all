package org.example;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class ReadPaimonInBatch {

    public static void main(String[] args) throws Exception {
        // ⭐ 关键：创建 Batch 模式的 TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inBatchMode()  // 👈 启用批处理模式
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 1. 注册 Paimon Catalog（与写入时一致）
        tEnv.executeSql(
                "CREATE CATALOG paimon_catalog WITH (" +
                        "  'type' = 'paimon'," +
                        "  'warehouse' = 'hdfs://localhost:8020/user/paimon'" +
                        ")"
        );

        // 2. 切换到 Paimon Catalog 和数据库
        tEnv.executeSql("USE CATALOG paimon_catalog");
        tEnv.executeSql("USE ods");

        // 3. 查询 Paimon 表（批模式 → 返回最终一致状态）
        tEnv.executeSql("SELECT sum(teacher_id) FROM paimon_courses")
                .print(); // 直接打印结果（每行是最终有效数据）
    }
}
