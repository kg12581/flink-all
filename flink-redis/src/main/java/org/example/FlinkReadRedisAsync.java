//package org.example;
//
//import org.apache.flink.streaming.api.datastream.AsyncDataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import redis.clients.jedis.Jedis;
//
//import java.util.concurrent.TimeUnit;
//
//public class FlinkReadRedisAsync {
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStream<Order> orderStream = ... // 来自 Kafka
//
//        // 异步查询 Redis
//        DataStream<OrderEnriched> enrichedStream = AsyncDataStream
//                .unorderedWait(
//                        orderStream,
//                        new RedisAsyncFunction(),
//                        1000, // 超时 1s
//                        TimeUnit.MILLISECONDS,
//                        100   // 最大并发
//                );
//
//        enrichedStream.print();
//        env.execute("Flink Async Lookup Redis");
//    }
//
//    // 异步函数
//    public static class RedisAsyncFunction
//            extends RichAsyncFunction<Order, OrderEnriched> {
//
//        private transient Jedis jedis;
//
//        @Override
//        public void open(Configuration parameters) {
//            jedis = new Jedis("localhost", 6379);
//            // jedis.auth("password");
//        }
//
//        @Override
//        public void asyncInvoke(Order input, ResultFuture<OrderEnriched> resultFuture) {
//            // 异步查询（实际用线程池，此处简化）
//            Thread thread = new Thread(() -> {
//                try {
//                    String userInfo = jedis.hget("user_profile:" + input.userId, "name");
//                    OrderEnriched enriched = new OrderEnriched(input, userInfo);
//                    resultFuture.complete(Collections.singletonList(enriched));
//                } catch (Exception e) {
//                    resultFuture.completeExceptionally(e);
//                }
//            });
//            thread.start();
//        }
//
//        @Override
//        public void close() {
//            if (jedis != null) jedis.close();
//        }
//    }
//}
