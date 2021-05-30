package com.shufang.streamapi;

import com.shufang.utils.MyUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;


public class StreamAPI_001_QuickStart {
    public static void main(String[] args) throws Exception {

        // 1 获取执行环境，程序的入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2 从数据源获取Source
        DataStreamSource<String> source = env.readTextFile(MyUtil.getPath("path1"));

        SingleOutputStreamOperator<Tuple3<String, String, Long>> mapStream = source.map(new MapFunction<String, Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple3<>(split[0], split[1], Long.parseLong(split[2]));
            }
        });

        mapStream.print();
        env.execute();
    }
}
