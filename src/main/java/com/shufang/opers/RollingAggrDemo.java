package com.shufang.opers;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class RollingAggrDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> source = env.readTextFile("src/main/resources/temps.txt");

        SingleOutputStreamOperator<SensorTemp> sensorTempStream = source.flatMap(new FlatMapFunction<String, SensorTemp>() {
            @Override
            public void flatMap(String s, Collector<SensorTemp> collector) throws Exception {
                String[] fields = s.split(",");
                collector.collect(new SensorTemp(fields[0], new Long(fields[1]), new Double(fields[2])));
            }
        });


        KeyedStream<SensorTemp, Tuple> keyedStream = sensorTempStream.keyBy("id");

        DataStream<SensorTemp> temp = keyedStream.max("temp");
        SingleOutputStreamOperator<SensorTemp> temp1 = keyedStream.maxBy("temp");

//        temp.print("temp");
//        temp1.print("temp1");


        SingleOutputStreamOperator<SensorTemp> resultStream = keyedStream.reduce((curState, newValue) -> {
            return new SensorTemp(curState.getId(), newValue.getTimestamp(), Math.max(curState.getTemp(), newValue.getTemp()));
        });

        resultStream.print("reduce");
        env.execute();
    }
}
