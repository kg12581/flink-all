package org.example;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.cj.x.protobuf.MysqlxResultset;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Properties;

public class KafkaToPaimonJob {

    public static void main(String[] args) throws Exception {

        // ============ 1. Flink 环境 ============
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(4);
        env.enableCheckpointing(60000);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // ============ 2. Kafka Source ============
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "paimon_group");

        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>("orders_topic", new SimpleStringSchema(), props);

        DataStream<String> kafkaStream = env
                .addSource(consumer)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                );

        // ============ 3. JSON 解析 ============
        ObjectMapper mapper = new ObjectMapper();

        SingleOutputStreamOperator<Row> rowStream = kafkaStream.map(value -> {
            JsonNode node = mapper.readTree(value);
            return Row.of(
                    node.get("order_id").asLong(),
                    node.get("user_id").asLong(),
                    node.get("amount").asDouble(),
                    node.get("status").asText()
            );
        }).returns(
                Types.ROW(
                        Types.LONG,
                        Types.LONG,
                        Types.DOUBLE,
                        Types.STRING
                )
        );

        // ============ 4. 注册 Table ============
        Schema schema = Schema.newBuilder()
                .column("order_id", DataTypes.BIGINT())
                .column("user_id", DataTypes.BIGINT())
                .column("amount", DataTypes.DOUBLE())
                .column("status", DataTypes.STRING())
                .build();

        Table orderTable = tEnv.fromDataStream(rowStream, schema);
        tEnv.createTemporaryView("kafka_orders", orderTable);

        // ============ 5. 创建 Paimon Catalog ============
        tEnv.executeSql(
                "CREATE CATALOG paimon_cat WITH (" +
                        "'type'='paimon'," +
                        "'warehouse'='file:/tmp/paimon'" +
                        ")"
        );

        tEnv.useCatalog("paimon_cat");

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS ods");
        tEnv.executeSql("USE ods");

        // ============ 6. 创建 Paimon 表 ============
        tEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS orders_paimon (" +
                        "order_id BIGINT," +
                        "user_id BIGINT," +
                        "amount DOUBLE," +
                        "status STRING," +
                        "PRIMARY KEY (order_id) NOT ENFORCED" +
                        ") WITH (" +
                        "'bucket'='4'," +
                        "'write-mode'='change-log'," +
                        "'changelog-producer'='input'" +
                        ")"
        );

        // ============ 7. 写入 Paimon ============
        tEnv.executeSql(
                "INSERT INTO orders_paimon " +
                        "SELECT order_id, user_id, amount, status FROM kafka_orders"
        );

        env.execute("Kafka To Paimon Job");
    }
}

