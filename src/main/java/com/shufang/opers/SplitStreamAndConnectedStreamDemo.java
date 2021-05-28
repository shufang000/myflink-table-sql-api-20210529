package com.shufang.opers;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;

public class SplitStreamAndConnectedStreamDemo {

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


        SplitStream<SensorTemp> split = sensorTempStream.split(new OutputSelector<SensorTemp>() {
            @Override
            public Iterable<String> select(SensorTemp value) {
                return value.getTemp() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SensorTemp> high = split.select("high");
        DataStream<SensorTemp> low = split.select("low");
        DataStream<SensorTemp> all = split.select("high","low");

//        high.print();
//        low.print();
//        all.print();


        /**
         * ConnectedStream只能同时连接2条流，每个流的类型可以不一样，但是最终输出的类型必须是一样的
         */
        ConnectedStreams<SensorTemp, SensorTemp> connectedStream = high.connect(low);
        
        connectedStream.map(new CoMapFunction<SensorTemp, SensorTemp, SensorTemp>() {
            @Override
            public SensorTemp map1(SensorTemp value) throws Exception {
                return value;
            }

            @Override
            public SensorTemp map2(SensorTemp value) throws Exception {
                return value;
            }
        }).print("connected stream");

        env.execute();
    }
}
