package com.shufang.table_sql;

import com.shufang.utils.MyUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Group Window
 * Over Window
 */
public class TableApiQuickStart_14_GroupWindow_03 {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        streamEnv.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, setting);
        String path1 = MyUtil.getPath("path1");
        System.out.println(path1);

        DataStreamSource<String> source = streamEnv.readTextFile(path1);

        //Justin,取消收藏,1622037255166
        SingleOutputStreamOperator<Tuple3<String, String, Long>> mapStream = source.map(new MapFunction<String, Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple3<>(split[0], split[1], Long.parseLong(split[2]));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element) {
                return element.f2;
            }
        });


        Table table = tableEnv.fromDataStream(mapStream, "name,action,action_time.rowtime");
        table.printSchema();

        Table select = table.window(Tumble.over("2.seconds").on("action_time").as("tw"))
                .groupBy("tw,name")
                .select("name,tw.end as twEnd,action.count as act");

        tableEnv.toAppendStream(select, Row.class).print("tw: ");


        streamEnv.execute();

    }
}
