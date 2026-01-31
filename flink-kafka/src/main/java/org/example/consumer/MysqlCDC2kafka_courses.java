package org.example.consumer;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class MysqlCDC2kafka_courses {
    public static void main(String[] args) throws Exception {


        Properties props = new Properties();
        props.setProperty("serverTimezone", "Asia/Shanghai");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        // 1. MySQL CDC Source
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("qianfeng")
                .tableList("qianfeng.courses")
                .username("root")
                .password("Admin@123456")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .serverTimeZone("Asia/Shanghai")
                .jdbcProperties(props)
                .build();

        DataStreamSource<String> stream = env.fromSource(
                mysqlSource,
                WatermarkStrategy.noWatermarks(),
                "mysql-cdc-source"
        );


        // 2. Kafka Sink
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("courses")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliverGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 3. 写入 Kafka
        stream.print();
        stream.sinkTo(kafkaSink);

        env.execute("MySQL CDC To Kafka Job");
    }
}
