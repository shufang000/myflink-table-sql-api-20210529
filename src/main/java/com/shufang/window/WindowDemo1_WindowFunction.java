package com.shufang.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.jetbrains.annotations.Nullable;

public class WindowDemo1_WindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> source = env.socketTextStream("shufang101", 9999);


        System.out.println(env.getStreamTimeCharacteristic());


        /**
         * 1、通常窗口是在end_time的时候关闭窗口，触发计算，丢弃数据
         * 2、如果设置了allowedLateness，那么当end_time的时候，窗口不会关闭，但是会马上输出一个窗口计算结果，但是在allowedLateness设置的允许
         * 迟到的范围内到的数据也会持续不断的更新窗口计算的结果，等到allowedLateness时间也到了，窗口也就关闭了，最终会输出一个近乎确定的结果
         * 3、再如果还需要对迟到数据做处理，可以使用sideOutputLateData侧输出流将窗口关闭后仍然属于窗口的数据进行处理，用ouptuttag进行标识
         */
//        OutputTag<String> side_output_stream_tag = new OutputTag<>("side output stream");
//        SingleOutputStreamOperator<Tuple2<String, Long>> id =


        SingleOutputStreamOperator<Tuple2<String, Double>> tupleStream = source.flatMap(new FlatMapFunction<String, Tuple2<String, Double>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Double>> collector) throws Exception {
                String[] words = s.split(",");
                collector.collect(new Tuple2<>(words[0], new Double(words[1])));
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> aggregate = tupleStream
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                //.trigger()
                //.evictor()
//        .allowedLateness(Time.seconds(3))  //这个只对EventTime的时间语义有效,
//        .sideOutputLateData(side_output_stream_tag)
                .aggregate(new AggregateFunction<Tuple2<String, Double>, Tuple2<String, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        return new Tuple2<>("",0L);
                    }

                    @Override
                    public Tuple2<String, Long> add(Tuple2<String, Double> stringDoubleTuple2, Tuple2<String, Long> stringLongTuple2) {
                        return new Tuple2<>(stringLongTuple2.f0, stringLongTuple2.f1 + 1);
                    }

                    @Override
                    public Tuple2<String, Long> getResult(Tuple2<String, Long> stringLongTuple2) {
                        return stringLongTuple2;
                    }

                    @Override
                    public Tuple2<String, Long> merge(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> acc1) {

                        return new Tuple2<>(acc1.f0, acc1.f1 + stringLongTuple2.f1);
                    }
                });


        aggregate.print();


        env.execute("nice window");
    }
}
