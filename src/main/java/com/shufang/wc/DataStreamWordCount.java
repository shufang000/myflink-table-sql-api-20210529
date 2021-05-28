package com.shufang.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamWordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host");
        Integer port = tool.getInt("port");

        DataStreamSource<String> source = env.socketTextStream(host,port);  // 默认并行度只能为1

        DataStream<Tuple2<String, Integer>> stream = source.flatMap(new DataSetWordCount.MyFlatMapFunction()).setParallelism(4);

        stream.keyBy(0).sum(1).print();

        env.execute("streaming word count");


    }


}
