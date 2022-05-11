package com.dajiangtai.batch.API;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

/**
 * 递归读取
 *
 * @author dajiangtai
 * @create 2019-07-29-10:30
 */
public class ReursionReadDemo {
    public static void main(String[] args) throws Exception {
        //获取一个执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //新建一个配置
        Configuration conf = new Configuration();

        //设置递归参数
        conf.setBoolean("recursive.file.enumeration",true);

        DataSet<String> ds = env.readTextFile("file:///D:\\study\\data\\2018")
                .withParameters(conf);

        ds.print();
    }
}
