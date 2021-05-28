package com.shufang.opers;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("src/main/resources/hello.txt");

        SingleOutputStreamOperator<Integer> mapStream = source.map((MapFunction<String, Integer>) String::length);

        mapStream.print();


        SingleOutputStreamOperator<String> flatMapStream = source.flatMap((FlatMapFunction<String, String>) (s, collector) -> {
            String[] word = s.split(" ");
            for (String s1 : word) {
                collector.collect(s1);
            }
        });


        SingleOutputStreamOperator<String> filter = flatMapStream.filter((FilterFunction<String>) s -> s.length() == 5);

        filter.print();

        flatMapStream.print();

        env.execute();
    }
}
