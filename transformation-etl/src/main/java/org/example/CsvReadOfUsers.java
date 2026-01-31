package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;
import org.example.bean.User;

public class CsvReadOfUsers {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println(env.getParallelism());
        // ✅ 关键：设置为 BATCH 模式（读取有限数据源如文件）
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // ✅ 使用 readTextFile（虽然 deprecated，但在 1.18 仍可用；未来建议用 DataStreamSource.fromCollection 或 Table API）
        DataStream<User> userStream = env
                .readTextFile("/Users/kgt/code/testmaven/flink-all/transformation-etl/src/main/resources/users.csv") // 确保该路径相对于项目根目录存在
                .map(line -> {
                    String[] fields = line.split(",");
                    Preconditions.checkArgument(fields.length == 3, "Invalid CSV line: " + line);
                    return new User(fields[0].trim(), fields[1].trim(), Integer.parseInt(fields[2].trim()));
                })
                .returns(TypeInformation.of(User.class));
        userStream.print();



//        FileSource<String> fileSource = FileSource
//                .forRecordStreamFormat(
//                        new TextLineInputFormat(),
//                        new Path("/Users/kgt/code/testmaven/flink-all/transformation-etl/src/main/resources/users.csv")
//                )
//                .build();
//
//        env
//                .fromSource(fileSource, WatermarkStrategy.noWatermarks(), "filesource")
//                .print();
        // 执行作业
        env.execute("Flink CSV Read Demo");
    }
}
