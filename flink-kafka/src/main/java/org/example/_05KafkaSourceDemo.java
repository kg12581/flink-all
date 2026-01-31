package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _05KafkaSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.execute();
    }

}
