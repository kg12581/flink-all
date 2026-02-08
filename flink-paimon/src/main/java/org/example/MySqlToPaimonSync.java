package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.*;

import java.util.Arrays;
import java.util.List;

public class MySqlToPaimonSync {

    public static void main(String[] args) throws Exception {
        // 1. 初始化 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(30_000); // 30s checkpoint

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 2. 配置 Paimon Catalog（作为结果存储）
        String warehousePath = "hdfs://localhost:8020/user/paimon"; // 生产建议用 HDFS/S3
        tEnv.executeSql(
                "CREATE CATALOG paimon_catalog WITH (" +
                        "  'type' = 'paimon'," +
                        "  'warehouse' = '" + warehousePath + "'" +
                        ")"
        );
        tEnv.useCatalog("paimon_catalog");

        // 3. 创建 MySQL CDC 源表（整库）
        // 注意：table-name 支持正则，"db1\\..*" 表示 db1 下所有表
        String createCdcSource =
                "CREATE TEMPORARY TABLE mysql_cdc_source (" +
                        "  `database_name` STRING METADATA VIRTUAL," +
                        "  `table_name` STRING METADATA VIRTUAL," +
                        "  `op_ts` TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL," +
                        "  `data` MAP<STRING, STRING> METADATA FROM 'values' VIRTUAL" +
                        ") WITH (" +
                        "  'connector' = 'mysql-cdc'," +
                        "  'hostname' = 'localhost'," +
                        "  'port' = '3306'," +
                        "  'username' = 'root'," +
                        "  'password' = 'Admin@123456'," +
                        "  'database-name' = 'test_db'," +          // 替换为你的数据库名
                        "  'table-name' = 'test_db\\\\..*'," +      // 正则：your_db 下所有表
                        "  'scan.incremental.snapshot.enabled' = 'false'," +
                        "  'server-id' = '5400-5404'" +
                        ")";

        tEnv.executeSql(createCdcSource);

        // 4. 【关键】获取所有表名（实际中可从元数据或配置读取）
        // 由于 Flink CDC 整库模式下无法直接列出表，我们假设已知表名列表
        // 或通过 Debezium 监控的表名配置
        List<String> tables = Arrays.asList("ods_order", "ods_prod_plan", "ods_prod_actual"
        ,"ods_inventory_flow","ods_bom","ods_material_cost","ods_quality"
        ); // 替换为你的表

        String database = "test_db";

        for (String table : tables) {
            String fullTableName = database + "." + table;
            String paimonTableName = table+"paimon"; // Paimon 表名（可加前缀）

            // 5. 动态创建 Paimon 表（结构与源表一致）
            // 注意：这里简化处理，实际应通过 CDC 元数据动态建表
            // 为演示，以 orders 表为例（你需根据实际结构调整）
            if ("ods_order".equals(table)) {
                tEnv.executeSql(
                        "CREATE TABLE IF NOT EXISTS `" + paimonTableName + "` (" +
                                "  order_id      VARCHAR(50),\n" +
                                "  factory_id    VARCHAR(20),\n" +
                                "  product_id    VARCHAR(20),\n" +
                                "  order_qty     INT,\n" +
                                "  order_amount  DECIMAL(18,2),\n" +
                                "  order_date    DATE,\n"+
                                "  PRIMARY KEY (order_id) NOT ENFORCED" +
                                ") WITH (" +
                                "  'bucket' = '2'" +
                                ")"
                );
            } else if ("ods_prod_plan".equals(table)) {
                tEnv.executeSql(
                        "CREATE TABLE IF NOT EXISTS `" + paimonTableName + "` (" +
                                "  factory_id VARCHAR(20),\n" +
                                "  product_id VARCHAR(20),\n" +
                                "  plan_qty   INT,\n" +
                                "  plan_date  DATE,\n"+
                                "  PRIMARY KEY (factory_id) NOT ENFORCED" +
                                ") WITH (" +
                                "  'bucket' = '2'" +
                                ")"
                );
            }else if ("ods_prod_actual".equals(table)) {
                tEnv.executeSql(
                        "CREATE TABLE IF NOT EXISTS `" + paimonTableName + "` (" +
                                "  factory_id VARCHAR(20),\n" +
                                "  product_id VARCHAR(20),\n" +
                                "  actual_qty INT,\n" +
                                "  prod_date  DATE ,\n"+
                                "  PRIMARY KEY (factory_id) NOT ENFORCED" +
                                ") WITH (" +
                                "  'bucket' = '2'" +
                                ")"
                );
            }else if ("ods_inventory_flow".equals(table)) {
                tEnv.executeSql(
                        "CREATE TABLE IF NOT EXISTS `" + paimonTableName + "` (" +
                                "  factory_id  VARCHAR(20),\n" +
                                "  product_id  VARCHAR(20),\n" +
                                "  change_qty  INT,\n" +
                                "  change_date DATE ,\n"+
                                "  PRIMARY KEY (factory_id) NOT ENFORCED" +
                                ") WITH (" +
                                "  'bucket' = '2'" +
                                ")"
                );
            }else if ("ods_bom".equals(table)) {
                tEnv.executeSql(
                        "CREATE TABLE IF NOT EXISTS `" + paimonTableName + "` (" +
                                "  parent_product VARCHAR(20),\n" +
                                "  child_material VARCHAR(20),\n" +
                                "  qty INT, \n"+
                                "  PRIMARY KEY (parent_product,child_material) NOT ENFORCED" +
                                ") WITH (" +
                                "  'bucket' = '2'" +
                                ")"
                );
            }else if ("ods_material_cost".equals(table)) {
                tEnv.executeSql(
                        "CREATE TABLE IF NOT EXISTS `" + paimonTableName + "` (" +
                                "  material_id VARCHAR(20),\n" +
                                "  unit_cost   DECIMAL(10,2),\n" +
                                "  PRIMARY KEY (material_id) NOT ENFORCED" +
                                ") WITH (" +
                                "  'bucket' = '2'" +
                                ")"
                );
            }else if ("ods_quality".equals(table)) {
                tEnv.executeSql(
                        "CREATE TABLE IF NOT EXISTS `" + paimonTableName + "` (" +
                                "  factory_id VARCHAR(20),\n" +
                                "  product_id VARCHAR(20),\n" +
                                "  bad_qty INT,\n" +
                                "  qc_date DATE ,\n" +
                                "  PRIMARY KEY (factory_id) NOT ENFORCED" +
                                ") WITH (" +
                                "  'bucket' = '2'" +
                                ")"
                );
            }
            // 可继续添加其他表...

            String selectClause;
            if ("ods_order".equals(table)) {
                selectClause =
                        "CAST(`data`['order_id'] AS STRING),\n" +
                                "CAST(`data`['factory_id'] AS STRING),\n" +
                                "CAST(`data`['product_id'] AS STRING),\n" +
                                "CAST(`data`['order_qty'] AS STRING),\n" +
                                "CAST(`data`['order_amount'] AS STRING),\n" +
                                "CAST(`data`['order_date'] AS STRING)";
            } else if ("ods_prod_plan".equals(table)) {
                selectClause =
                        "CAST(`data`['factory_id'] AS STRING),\n" +
                                "CAST(`data`['product_id'] AS STRING),\n" +
                                "CAST(`data`['plan_qty'] AS STRING),\n" +
                                "CAST(`data`['plan_date'] AS STRING)";
            }else if ("ods_prod_actual".equals(table)) {
                selectClause =
                        "CAST(`data`['factory_id'] AS STRING),\n" +
                                "CAST(`data`['product_id'] AS STRING),\n" +
                                "CAST(`data`['actual_qty'] AS STRING),\n" +
                                "CAST(`data`['prod_date'] AS STRING)";
            }else if ("ods_inventory_flow".equals(table)) {
                selectClause =
                        "CAST(`data`['factory_id'] AS STRING),\n" +
                                "CAST(`data`['product_id'] AS STRING),\n" +
                                "CAST(`data`['change_qty'] AS STRING),\n" +
                                "CAST(`data`['change_date'] AS STRING)";
            }else if ("ods_bom".equals(table)) {
                selectClause =
                        "CAST(`data`['parent_product'] AS STRING),\n" +
                                "CAST(`data`['child_material'] AS STRING),\n" +
                                "CAST(`data`['qty'] AS STRING)\n";
            }else if ("ods_material_cost".equals(table)) {
                selectClause =
                        "CAST(`data`['material_id'] AS STRING),\n" +
                                "CAST(`data`['unit_cost'] AS STRING)";
            }else if ("ods_quality".equals(table)) {
                selectClause =
                        "CAST(`data`['factory_id'] AS STRING),\n" +
                                "CAST(`data`['product_id'] AS STRING),\n" +
                                "CAST(`data`['bad_qty'] AS STRING),\n" +
                                "CAST(`data`['qc_date'] AS STRING)";
            } else {
                throw new IllegalArgumentException("Unsupported table: " + table);
            }
            // 6. 启动同步任务：从 CDC 流过滤出当前表，写入 Paimon
            String insertSql =
                    "INSERT INTO `" + paimonTableName + "` " +
                            "SELECT " + selectClause +
                            "FROM mysql_cdc_source " +
                            "WHERE `database_name` = '" + database + "' " +
                            "  AND `table_name` = '" +  table+ "'";

            // 注意：上面 SELECT 需根据表结构调整！
            // 更通用的方式是使用 Flink CDC 的 FLATTEN 模式（见下方说明）

            tEnv.executeSql(insertSql);
        }

        // 7. 提交作业
        env.execute("MySQL to Paimon Full Database Sync");
    }
}
