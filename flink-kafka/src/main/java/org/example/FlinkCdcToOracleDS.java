//package org.example.producer;
//
////import com.ververica.cdc.connectors.mysql.MySQLSource;
//import com.ververica.cdc.connectors.mysql.source.MySqlSource;
//import com.ververica.cdc.connectors.mysql.table.StartupOptions;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//import org.apache.flink.connector.jdbc.JdbcSink;
//import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.types.Row;
//import org.apache.kafka.connect.data.Struct;
//import org.apache.kafka.connect.source.SourceRecord;
//import org.apache.flink.streaming.api.datastream.DataStream;
//
//import java.sql.PreparedStatement;
//import java.sql.SQLException;
//
//public class FlinkCdcToOracleDS {
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        // ==============================
//        // 1. 创建 MySQL CDC Source
//        // ==============================
//        MySqlSource<String> mySQLSource = MySqlSource.<String>builder()
//                .hostname("127.0.0.1")
//                .port(3306)
//                .databaseList("testdb") // MySQL 数据库
//                .tableList("testdb.user") // 表名
//                .username("root")
//                .password("root")
//                .startupOptions(StartupOptions.latest())
//                .deserializer(new com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema()) // 输出 JSON 字符串
//                .build();
//
//        DataStream<String> mysqlStream = env.fromSource(
//                mySQLSource,
//                WatermarkStrategy.noWatermarks(),
//                "MySQL CDC Source"
//        );
//
//        // ==============================
//        // 2. 转成 Row 对象
//        // ==============================
//        DataStream<Row> rowStream = mysqlStream.map(new MapFunction<String, Row>() {
//            @Override
//            public Row map(String value) throws Exception {
//                // JSON 示例: {"before":null,"after":{"id":1,"name":"Alice","status":1},"op":"c"}
//                com.fasterxml.jackson.databind.JsonNode node =
//                        new com.fasterxml.jackson.databind.ObjectMapper().readTree(value);
//                com.fasterxml.jackson.databind.JsonNode after = node.get("after");
//                if (after == null || after.isNull()) {
//                    return null; // 删除事件可忽略
//                }
//                Row row = new Row(3);
//                row.setField(0, after.get("id").asInt());
//                row.setField(1, after.get("name").asText());
//                row.setField(2, after.get("status").asInt());
//                return row;
//            }
//        }).returns(new RowTypeInfo(
//                TypeInformation.of(Integer.class),
//                TypeInformation.of(String.class),
//                TypeInformation.of(Integer.class)
//        )).filter(r -> r != null);
//
//        // ==============================
//        // 3. 写入 Oracle
//        // ==============================
//        String upsertSql = "MERGE INTO FLINK_SINK_USER t " +
//                "USING (SELECT ? AS id, ? AS name, ? AS status FROM dual) s " +
//                "ON (t.id = s.id) " +
//                "WHEN MATCHED THEN UPDATE SET t.name = s.name, t.status = s.status " +
//                "WHEN NOT MATCHED THEN INSERT (id, name, status) VALUES (s.id, s.name, s.status)";
//
//        rowStream.addSink(
//                JdbcSink.sink(
//                        upsertSql,
//                        new JdbcStatementBuilder<Row>() {
//                            @Override
//                            public void accept(PreparedStatement ps, Row row) throws SQLException {
//                                ps.setInt(1, (Integer) row.getField(0));
//                                ps.setString(2, (String) row.getField(1));
//                                ps.setInt(3, (Integer) row.getField(2));
//                            }
//                        },
//                        new org.apache.flink.connector.jdbc.JdbcExecutionOptions.Builder()
//                                .withBatchSize(500)
//                                .withBatchIntervalMs(2000)
//                                .build(),
//                        new org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                                .withUrl("jdbc:oracle:thin:@127.0.0.1:1521/XE")
//                                .withDriverName("oracle.jdbc.OracleDriver")
//                                .withUsername("ogg2")
//                                .withPassword("ogg2")
//                                .build()
//                )
//        );
//
//        env.execute("Flink CDC MySQL to Oracle DS");
//    }
//}
//
