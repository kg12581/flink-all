package org.example;

// Flink core
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// Kafka
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

// JSON
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

// Paimon catalog & schema
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;

// Paimon sink
//import org.apache.paimon.flink.sink.PaimonSink;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.flink.api.connector.sink.Sink;

// Flink table runtime (RowData)
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

// Hadoop Path
import org.example.bean.Order;

// Java
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


public class KafkaToPaimonDataStreamJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);
        env.setParallelism(2);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("orders")
                .setGroupId("flink-paimon-ds")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<Order> orderStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);
                    Order o = new Order();
                    o.orderId = obj.getString("order_id");
                    o.userId = obj.getString("user_id");
                    o.amount = obj.getDoubleValue("amount");
                    o.ts = obj.getLongValue("ts");
                    return o;
                });

        Map<String, String> options = new HashMap<>();
        options.put("warehouse", "hdfs://namenode:8020/paimon");

        CatalogContext context = CatalogContext.create((Path) options);
        Catalog catalog = CatalogFactory.createCatalog(context);


        Identifier tableId = Identifier.create("default", "paimon_orders");

        if (!catalog.tableExists(tableId)) {
            Schema schema = Schema.newBuilder()
                    .column("order_id", DataTypes.STRING())
                    .column("user_id", DataTypes.STRING())
                    .column("amount", DataTypes.DOUBLE())
                    .column("ts", DataTypes.BIGINT())
                    .primaryKey("order_id")
                    .option("bucket", "4")
                    .build();

            catalog.createTable(tableId, schema, false);
        }

        DataStream<RowData> rowDataStream = orderStream.map(order -> {
            GenericRowData row = new GenericRowData(4);
            row.setField(0, StringData.fromString(order.orderId));
            row.setField(1, StringData.fromString(order.userId));
            row.setField(2, order.amount);
            row.setField(3, order.ts);
            return row;
        });

        Table table = catalog.getTable(
                Identifier.create("default", "paimon_orders")
        );

//        Sink<RowData> sink = new FlinkSinkBuilder(table).build();

//        rowDataStream.sinkTo(sink);

        env.execute("Kafka To Paimon DataStream Job");
    }
}

