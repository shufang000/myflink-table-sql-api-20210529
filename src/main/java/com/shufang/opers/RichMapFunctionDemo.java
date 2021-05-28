package com.shufang.opers;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;



public class RichMapFunctionDemo {
    public static void main(String[] args) {

    }


    /**
     * 通常oper的每个subtask都会创建一个MyMapper的实例，通常open\close方法的调用次数和 stream的partition数量保持一致
     */
    class MyMapper extends RichMapFunction<String, Tuple2<String,Integer>>{
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public Tuple2<String, Integer> map(String s) throws Exception {
            return null;
        }
    }
}
