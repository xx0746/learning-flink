package com.dajiangtai.batch.API;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * 数据union操作
 *
 * @author dajiangtai
 * @create 2019-07-29-11:10
 */
public class UnionDemo {

    public static void main(String[] args) throws Exception {
        //获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer,String>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>(101,"lily"));
        list1.add(new Tuple2<>(102,"lucy"));
        list1.add(new Tuple2<>(103,"tom"));

        ArrayList<Tuple2<Integer,String>> list2 = new ArrayList<>();
        list2.add(new Tuple2<>(101,"lili"));
        list2.add(new Tuple2<>(102,"jack"));
        list2.add(new Tuple2<>(103,"jetty"));

        DataSet<Tuple2<Integer, String>> ds1 = env.fromCollection(list1);
        DataSet<Tuple2<Integer, String>> ds2 = env.fromCollection(list2);

        DataSet<Tuple2<Integer, String>> union = ds1.union(ds2);

        union.print();
    }
}
