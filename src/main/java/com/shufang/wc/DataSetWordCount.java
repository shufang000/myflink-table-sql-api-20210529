package com.shufang.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class DataSetWordCount {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = "src/main/resources/hello.txt";

        DataSource<String> dataset = env.readTextFile(filePath);

        FlatMapOperator<String, Tuple2<String, Integer>> wordMap1 = dataset.flatMap(new MyFlatMapFunction());


        AggregateOperator<Tuple2<String, Integer>> wc = wordMap1.groupBy(0).sum(1);

        wc.print();
    }


    //这是一个内部类
    static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }

}
