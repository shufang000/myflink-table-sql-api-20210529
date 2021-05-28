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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * Group Window
 * Over Window
 */
public class TableApiQuickStart_15_GroupWindow_04 {
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


        String ddl = "CREATE TABLE user_action(" +
                "name STRING, " +
                "action STRING, " +
                "action_time BIGINT, " +
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(action_time/1000,'yyyy-MM-dd HH:mm:ss')), " +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '0' SECOND " +
                ") WITH (" +
                "'connector.type' = 'filesystem'," +
                "'connector.path' = '" + path1 + "'," +
                "'format.type' = 'csv'" +
                ")";

        tableEnv.sqlUpdate(ddl);


        Table select = tableEnv.sqlQuery("SELECT name,TUMBLE_END(rowtime,INTERVAL '60' SECOND),COUNT(action)" +
                "  FROM user_action GROUP BY TUMBLE(rowtime,INTERVAL '60' SECOND),name");

        select.printSchema();

        tableEnv.toAppendStream(select,Row.class).print();


        streamEnv.execute();

    }
}
