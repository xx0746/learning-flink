package com.dajiangtai.batch.API;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.lang.reflect.Executable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * 分布式缓存
 *
 * @author dajiangtai
 * @create 2019-07-29-14:16
 */
public class DistributeCacheDemo {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //注册一个本地可执行文件，用户基本数据
        env.registerCachedFile("file:///E:\\工作资料\\大讲台资料汇总\\大讲台教学相关\\大讲台课程相关\\flink 课程\\Flink 实时数仓项目实战\\数据源\\user.txt","localFile",true);

        //准备用户游戏充值数据
        ArrayList<Tuple2<String,Integer>> data = new ArrayList<>();
        data.add(new Tuple2<>("101",2000000));
        data.add(new Tuple2<>("102",190000));
        data.add(new Tuple2<>("103",1090000));


        //读取数据源
        DataSet<Tuple2<String, Integer>> tuple2DataSource = env.fromCollection(data);

        DataSet<String> result = tuple2DataSource.map(new RichMapFunction<Tuple2<String, Integer>, String>() {
            HashMap<String,String> allMap = new HashMap<String,String>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                File localFile = getRuntimeContext().getDistributedCache().getFile("localFile");
                List<String> lines = FileUtils.readLines(localFile);

                for (String line:lines){
                    String[] split = line.split(",");
                    allMap.put(split[0],split[1]);
                }
            }

            @Override
            public String map(Tuple2<String, Integer> t) throws Exception {
                String name = allMap.get(t.f0);
                return name+","+t.f1;
            }
        });
        result.print();
    }
}
