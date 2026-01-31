package org.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class FlinkWriteRedis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟数据流：(userId, action)
        DataStream<Tuple2<String, String>> stream = env.fromElements(
                Tuple2.of("user1", "click"),
                Tuple2.of("user2", "view")
        );

        // Redis 连接配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                // .setPassword("your_password") // 如有密码
                .build();

        // 添加 Redis Sink
        stream.addSink(new RedisSink<>(conf, new MyRedisMapper()));

        env.execute("Flink Write to Redis");
    }

    // 定义 Redis 写入映射
    public static class MyRedisMapper implements RedisMapper<Tuple2<String, String>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            // 使用 HSET: key = "user_actions:{userId}", field = timestamp, value = action
            return new RedisCommandDescription(RedisCommand.HSET, "user_actions:");
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0; // userId
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1; // action
        }
    }
}