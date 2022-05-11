package com.dajiangtai.batch.API;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * 读取csv文件示例
 *
 * @author dajiangtai
 * @create 2019-07-29-10:12
 */
public class ReadCSVDemo {
    public static void main(String[] args) throws Exception {
        //获取一个执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //读取输入数据
        DataSet<Tuple3<Integer,Integer,String>> csvDS = env.readCsvFile("E:\\工作资料\\大讲台资料汇总\\大讲台教学相关\\大讲台课程相关\\flink 课程\\Flink 实时数仓项目实战\\数据源\\user.csv")
                .includeFields("11100")
                .ignoreFirstLine()
                //.ignoreInvalidLines()
                .ignoreComments("##")
                .lineDelimiter("\n")
                .fieldDelimiter(",")
                .types(Integer.class,Integer.class,String.class);

        csvDS.print();
    }
}
