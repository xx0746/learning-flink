package com.dajiangtai.stream.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * 二次开发
 *
 * @author dajiangtai
 * @create 2019-06-09-17:03
 */
public class MyRedisSink {
    public static void main(String[] args) throws Exception{
        //获取一个执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.socketTextStream("192.168.20.210", 9999, "\n");

        DataStream<Tuple2<String,String>> redis_wordsData =  dataStream.map(new MapFunction<String, Tuple2<String,String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                return new Tuple2<>("reids_words",s);
            }
        });

        FlinkJedisPoolConfig build = new FlinkJedisPoolConfig.Builder().setHost("192.168.20.210").setPort(6379).build();

       RedisSink<Tuple2<String,String>> redisSink =  new RedisSink<>(build,new MyRedisMapper());

        redis_wordsData.addSink(redisSink);

        env.execute("MyRedisSink");
    }

    public static class  MyRedisMapper implements RedisMapper<Tuple2<String,String>>{
        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }
    }
}
