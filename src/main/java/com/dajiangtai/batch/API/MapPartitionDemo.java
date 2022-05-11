package com.dajiangtai.batch.API;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

/**
 * mappartition测试
 *
 * @author dajiangtai
 * @create 2019-07-29-10:44
 */
public class MapPartitionDemo {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //产生数据
        DataSet<Long> ds = env.generateSequence(1, 20);

        ds.mapPartition(new MyMapPartitionFunciton()).print();
    }

    public static class MyMapPartitionFunciton implements MapPartitionFunction<Long,Long>{
        @Override
        public void mapPartition(Iterable<Long> values, Collector<Long> out) throws Exception {
            long count =0;
            for(Long value:values){
                count++;
            }
            out.collect(count);
        }
    }
}
