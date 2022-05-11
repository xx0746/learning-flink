package com.dajiangtai.batch.API;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 第一个批处理示例程序
 *
 * @author dajiangtai
 * @create 2019-07-29-9:43
 */
public class BatchWordCountDemo {
    public static void main(String[] args) throws Exception {
        //获取一个执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //读取输入数据
        DataSet<String> ds = env.fromElements("flink flink flink","spark spark spark");

        //单词的词频统计
        DataSet<Tuple2<String, Integer>> sum = ds.flatMap(new LineSplitter())
                .groupBy(0)
                .sum(1);

        sum.print();
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String,Integer>>{
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
            for(String word:line.split(" ")){
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
