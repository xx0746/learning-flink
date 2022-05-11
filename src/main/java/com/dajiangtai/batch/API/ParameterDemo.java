package com.dajiangtai.batch.API;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.configuration.Configuration;

/**
 * 参数传递
 *
 * @author dajiangtai
 * @create 2019-07-29-14:37
 */
public class ParameterDemo {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataSet<Integer> data = env.fromElements(1, 2, 3, 4, 5);


        /**
         * 构造方法传递参数
         */
        //DataSet<Integer> filter = data.filter(new MyFilter(3));
        //filter.print();

        /**
         * Configuration传递参数
         */
//        Configuration conf = new Configuration();
//        conf.setInteger("limit",3);
//
//        DataSet<Integer> filter = data.filter(new RichFilterFunction<Integer>() {
//            private  int limit;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                limit = parameters.getInteger("limit",0);
//            }
//
//            @Override
//            public boolean filter(Integer value) throws Exception {
//                return value>limit;
//            }
//        }).withParameters(conf);
//        filter.print();

        Configuration conf = new Configuration();
        conf.setInteger("limit",3);

        env.getConfig().setGlobalJobParameters(conf);

        DataSet<Integer> filter = data.filter(new RichFilterFunction<Integer>() {
            private int limit ;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ExecutionConfig.GlobalJobParameters globalJobParameters = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                Configuration globalConf = (Configuration) globalJobParameters;
                limit = globalConf.getInteger("limit",0);
            }

            @Override
            public boolean filter(Integer value) throws Exception {
                return value>limit;
            }
        });
        filter.print();


    }

//    public static class MyFilter implements FilterFunction<Integer>{
//        private  int limit = 0;
//
//        public MyFilter(int limit){
//            this.limit = limit;
//        }
//
//        @Override
//        public boolean filter(Integer value) throws Exception {
//            return value>limit;
//        }
//    }
}
