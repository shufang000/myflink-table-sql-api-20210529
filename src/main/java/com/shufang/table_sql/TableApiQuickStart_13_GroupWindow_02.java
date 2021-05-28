package com.shufang.table_sql;

import com.shufang.utils.MyUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * Group Window
 * Over Window
 */
public class TableApiQuickStart_13_GroupWindow_02 {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, setting);
        String path1 = MyUtil.getPath("path1");
        System.out.println(path1);


        //Ben,付款,1622037247149
        String ddl = "CREATE TABLE user_action ( \n" +
                "name STRING, \n" +
                "action STRING, \n" +
                "action_time BIGINT, \n" +
                "proctime AS PROCTIME() \n" +
                ") WITH ( \n" +
                "'connector.type' = 'filesystem',  \n" +
                "'connector.path' ='" + path1 + "',\n" +
                "'format.type' = 'csv',\n" +
                "'update-mode' = 'append'\n" +
                ")";


        tableEnv.sqlUpdate(ddl);
        Table user_action = tableEnv.from("user_action");

        user_action.printSchema();

        //tableEnv.toAppendStream(user_action, Row.class).print("user_action");

        Table select = user_action
                .window(Tumble.over("1.seconds").on("proctime").as("w"))
                .groupBy("w,name")
                .select("name,w.end,name.count");

        select.printSchema();

        tableEnv.toRetractStream(select, Row.class).print();

        streamEnv.execute();

    }
}
