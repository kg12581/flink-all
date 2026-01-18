package org.example;

import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.sink.DorisSink;

import java.util.Arrays;
import java.util.Properties;

public class FlinkToDorisDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        // 1) upstream 数据
        DataStreamSource<String> source = env.fromCollection(Arrays.asList(
                "1,Tom,25",
                "2,Lucy,30",
                "3,Bob,22"
        ));

        DataStream<Tuple3<Integer, String, Integer>> tupleStream = source
                .map(line -> {
                    String[] arr = line.split(",");
                    return new Tuple3<>(
                            Integer.parseInt(arr[0]),
                            arr[1],
                            Integer.parseInt(arr[2])
                    );
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<Integer,String,Integer>>(){}));

        // 2) Doris 配置
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes("127.0.0.1:8030")
                .setTableIdentifier("test.test_flink")
                .setUsername("root")
                .setPassword("")
                .build();

        // 3) StreamLoad 参数
        Properties streamProps = new Properties();
        streamProps.setProperty("format", "json");
        streamProps.setProperty("read_json_by_line", "true");

        DorisExecutionOptions execOpts = DorisExecutionOptions.builder()
                .setLabelPrefix("flink_doris_label")
                .setStreamLoadProp(streamProps)
                .build();

        // 4) 构建 DorisSink 并设置自定义序列化器
        DorisSink<Tuple3<Integer, String, Integer>> sink = DorisSink.<Tuple3<Integer,String,Integer>>builder()
                .setDorisOptions(dorisOptions)
                .setDorisExecutionOptions(execOpts)
                .setSerializer((DorisRecordSerializer<Tuple3<Integer, String, Integer>>) new MyDorisSerializer())  // 👈 使用自定义实现
                .build();

        tupleStream.sinkTo(sink);

        env.execute("Flink Write Doris Demo 25.1.0");
    }
}


