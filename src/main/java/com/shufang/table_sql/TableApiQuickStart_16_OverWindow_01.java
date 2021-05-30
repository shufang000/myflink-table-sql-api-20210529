package com.shufang.table_sql;

import com.shufang.utils.MyUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * Over Window : 类似于Hive中的 agg() over(partition by attr order by attr rows between n preceding and current row,
 * [n following])
 */
public class TableApiQuickStart_16_OverWindow_01 {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        streamEnv.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, setting);
        String path1 = MyUtil.getPath("path1");
        System.out.println(path1);


        /** 目标
         * root
         *  |-- name: STRING
         *  |-- action: STRING
         *  |-- action_time: TIMESTAMP(3) *ROWTIME*
         */

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


        Table user_action = tableEnv.fromDataStream(mapStream, "name,action,action_time.rowtime");
        tableEnv.createTemporaryView("user_action",user_action); //注册

        user_action.printSchema();

       /* *//**
         * 方式一
         *//*
        Table select = user_action.window(Over.partitionBy("name").orderBy("action_time").preceding("UNBOUNDED_ROW").as("w"))
                .select("name,action,action_time,action.count over w");

        *//**
         * 方式二
         *//*
        Table select1 = tableEnv.sqlQuery("SELECT \n" +
                "name,\n" +
                "action_time,\n" +
                "action,\n" +
                "count(action) over w, \n" +
                "sum(2) over w \n" +
                "FROM user_action \n" +
                "window w as(\n" +
                "partition by name \n" +
                "order by action_time\n" +
                ")");*/

        /**
         * 方式三 ,row_number 之后必须指定区间 rk <= 或者 < 或者 =
         */
        Table select2 = tableEnv.sqlQuery("SELECT\n" +
                "name,\n" +
                "*" +
                "FROM(\t\n" +
                "SELECT \n" +
                "name,\n" +
                "action,\n" +
                "action_time,\n" +
                "row_number() over(partition by name order by action_time) as rk\n" +
                "FROM user_action\n" +
                ")t WHERE rk <= 3");

        tableEnv.toRetractStream(select2,Row.class).print();

        streamEnv.execute();

    }
}
