package org.example;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.source.DorisSource;

import java.util.Collections;

public class FlinkFromDorisDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. DorisOptions 正确写法
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes(Collections.singletonList("127.0.0.1:8030").toString()) // ✅ 注意这里是 List<String>
                .setTableIdentifier("test.test_flink")
                .setUsername("root")
                .setPassword("")
                .build();

        // 2. DorisReadOptions 可以用默认
        DorisReadOptions readOptions = DorisReadOptions.builder().build();

        // 3. 构建 DorisSource
        DorisSource<Tuple3<Integer,String,Integer>> dorisSource =
                DorisSource.<Tuple3<Integer,String,Integer>>builder()
                        .setDorisOptions(dorisOptions)
                        .setDorisReadOptions(readOptions)
//                        .setQuery("SELECT id, name, age FROM test.test_flink")
//                        .setRowTypeInfo(TypeInformation.of(new TypeHint<Tuple3<Integer,String,Integer>>(){}))
                        .build();

        // 4. 读取数据
        DataStream<Tuple3<Integer,String,Integer>> stream = env
                .fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "Doris Source");

        stream.print();

        env.execute("Flink Read Doris Demo 25.1.0");
    }
}
