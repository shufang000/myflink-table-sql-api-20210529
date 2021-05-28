package com.shufang.window;

import com.shufang.beans.SensorTemper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.util.OutputTag;

public class WindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


//        DataStreamSource<String> source = env.readTextFile("src/main/resources/sensor.txt");

        DataStreamSource<String> source = env.socketTextStream("shufang101", 9999);
        SingleOutputStreamOperator<SensorTemper> sensorTemperStream = source.map(new MapFunction<String, SensorTemper>() {
            @Override
            public SensorTemper map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorTemper(fields[0], new Double(fields[1]));
            }
        });

        SingleOutputStreamOperator<SensorTemper> result = sensorTemperStream.keyBy("id")
//                  .timeWindow(Time.seconds(5))
//                  .countWindow()
//                  .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                  .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(10)))
//                  .windowAll()
//                  .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .timeWindow(Time.seconds(1))
                .minBy("id");


        result.print();


//        DataStream<SensorTemper> late_ones = result.getSideOutput(new OutputTag<SensorTemper>("late ones"));


        env.execute();


    }
}
