package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CourseTeacherJoin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // === 1. 创建 courses CDC 源表 ===
        tEnv.executeSql(
                "CREATE TABLE courses_cdc (" +
                        "  course_id INT," +
                        "  course_name STRING," +
                        "  teacher_id INT," +
                        "  category STRING," +
                        "  difficulty STRING," +
                        "  PRIMARY KEY (course_id) NOT ENFORCED" +
                        ") WITH (" +
                        "  'connector' = 'mysql-cdc'," +
                        "  'hostname' = 'localhost'," +
                        "  'port' = '3306'," +
                        "  'username' = 'root'," +
                        "  'password' = 'Admin@123456'," +
                        "  'database-name' = 'qianfeng'," +
                        "  'table-name' = 'courses'" +
                        ")"
        );

        // === 2. 创建 teachers CDC 源表 ===
        tEnv.executeSql(
                "CREATE TABLE teachers_cdc (" +
                        "  teacher_id INT," +
                        "  teacher_name STRING," +
                        "  hire_date DATE," +
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

        // === 3. 执行双流 JOIN 查询 ===
        // 注意：Flink 会自动将两个 changelog 流进行基于主键的关联
        tEnv.executeSql(
                "SELECT " +
                        "  c.course_name," +
                        "  t.teacher_name," +
                        "  t.department," +
                        "  c.category," +
                        "  c.difficulty " +
                        "FROM courses_cdc AS c " +
                        "JOIN teachers_cdc AS t " +
                        "ON c.teacher_id = t.teacher_id"
        ).print(); // 输出到控制台（用于调试）

        // 程序会持续运行，监听 MySQL 变更并输出最新关联结果
    }
}