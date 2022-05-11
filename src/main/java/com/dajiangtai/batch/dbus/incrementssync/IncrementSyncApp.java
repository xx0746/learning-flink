package com.dajiangtai.batch.dbus.incrementssync;

import com.alibaba.otter.canal.protocol.FlatMessage;
import com.dajiangtai.batch.dbus.function.DbusProcessFunction;
import com.dajiangtai.batch.dbus.model.Flow;
import com.dajiangtai.batch.dbus.schema.FlatMessageSchema;
import com.dajiangtai.batch.dbus.sink.HbaseSyncSink;
import com.dajiangtai.batch.dbus.source.FlowSoure;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * 实时增量同步模块
 *
 * @author dajiangtai
 * @create 2019-07-31-11:04
 */
public class IncrementSyncApp {

    public static final MapStateDescriptor<String, Flow> flowStateDescriptor =
            new MapStateDescriptor<String, Flow>("flowBroadCastState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<Flow>() {
            }));
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "master:9092");
        props.put("zookeeper.connect", "master:2181");
        props.put("group.id", "group1");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        props.put("flink.partition-discovery.interval-millis","30000");

        //消费kafka数据
        FlinkKafkaConsumer010<FlatMessage> myConsumer = new FlinkKafkaConsumer010<>("test", new FlatMessageSchema(), props);
        DataStream<FlatMessage> message = sEnv.addSource(myConsumer);


        //同库，同表数据进入同一个分组，一个分区
        KeyedStream<FlatMessage, String> keyedMessage = message.keyBy(new KeySelector<FlatMessage, String>() {
            @Override
            public String getKey(FlatMessage value) throws Exception {
                return value.getDatabase() + value.getTable();
            }
        });

        //读取配置流
        BroadcastStream<Flow> broadcast = sEnv.addSource(new FlowSoure()).broadcast(flowStateDescriptor);

        //连接数据流和配置流
        DataStream<Tuple2<FlatMessage,Flow>> connectedStream = keyedMessage.connect(broadcast)
                .process(new DbusProcessFunction())
                .setParallelism(1);


        connectedStream.addSink(new HbaseSyncSink());

        sEnv.execute("IncrementSyncApp");
    }
}
