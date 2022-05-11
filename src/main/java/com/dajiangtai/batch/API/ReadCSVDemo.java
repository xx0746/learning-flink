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
        DataSet<Tuple3<Integer,Integer,String>> csvDS = env.readCsvFile("C:\\Users\\seansheen\\Desktop\\spring代码\\947de11dba327c042eb45c7f58f978c7\\Flink1.8实时数仓项目实战【314190】数据去重实操\\Flink 实时数仓项目实战-完整资料\\数据源\\user.csv")
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
