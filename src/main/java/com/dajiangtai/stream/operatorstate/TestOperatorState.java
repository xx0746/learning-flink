package com.dajiangtai.stream.operatorstate;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestOperatorState {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置checkpoint
        env.enableCheckpointing(60000L);
        CheckpointConfig checkpointConfig=env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(30000L);
        checkpointConfig.setCheckpointTimeout(10000L);
        checkpointConfig.setFailOnCheckpointingErrors(true);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);


        DataStream<Long> inputStream=env.fromElements(1L,2L,3L,4L,5L,1L,3L,4L,5L,6L,7L,1L,4L,5L,3L,9L,9L,2L,1L);

        inputStream.flatMap(new CountWithOperatorState())
                .setParallelism(1)
                .print();

        env.execute();

    }
}
