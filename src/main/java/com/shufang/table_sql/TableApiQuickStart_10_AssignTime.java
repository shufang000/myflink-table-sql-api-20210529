package com.shufang.table_sql;

import com.shufang.utils.MyUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableApiQuickStart_10_AssignTime {
    public static void main(String[] args) throws Exception {

        EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, setting);
        String path1 = MyUtil.getPath("path1");
        System.out.println(path1);


        //Ben,付款,1622037247149
        String ddl = "CREATE TABLE user_action ( \n" +
                "name STRING, \n" +
                "action STRING, \n" +
                "action_time BIGINT, \n" +
                "rts AS TO_TIMESTAMP(FROM_UNIXTIME(action_time/1000,'YYYY-MM-dd hh:mm:ss')), \n" +
                "WATERMARK FOR rts AS rts - INTERVAL '2' SECOND \n" +
                ") WITH ( \n" +
                "'connector.type' = 'filesystem',  \n" +
                "'connector.path' ='" + path1 + "',\n" +
                "'format.type' = 'csv',\n" +
                "'update-mode' = 'append'\n" +
                ")";

        tableEnv.sqlUpdate(ddl);
        Table user_action = tableEnv.from("user_action");

        user_action.printSchema();

        tableEnv.toAppendStream(user_action, Row.class).print("csv");

        streamEnv.execute("");

    }
}
