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
public class TableApiQuickStart_12_GroupWindow {
    public static void main(String[] args) throws Exception {

        EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, setting);
        String path2 = MyUtil.getPath("path2");

        //10:15,2,Euro
        tableEnv.connect(new FileSystem().path(path2))
                .withFormat(new Csv().ignoreParseErrors())
                .withSchema(new Schema()
                        .field("rowtime", DataTypes.STRING())
                        .field("amount", DataTypes.INT())
                        .field("currency", DataTypes.STRING())
                        .field("proctime", DataTypes.TIMESTAMP(3)).proctime()
                ).createTemporaryTable("orders");
        Table orders = tableEnv.from("orders");

        /**
         * 1 使用TableAPI指定GroupWindow，与DataStream API中的窗口含义一样，包含滚动、滑动、会话窗口
         */
        // 1.1 滚动窗口
        orders.window(Tumble.over("10.seconds").on("rowtime").as("w")); //EventTime  Tumble
        orders.window(Tumble.over("10.seconds").on("proctime").as("w")); //Processing Time Tumble
        orders.window(Tumble.over("10.rows").on("proctime").as("w")); //Count Tumble

        // 1.2 滑动窗口
        orders.window(Slide.over("10.seconds").every("5.seconds").on("rowtime").as("w")); //EventTime Slide
        orders.window(Slide.over("10.seconds").every("5.seconds").on("proctime").as("w")); //Processing Time Slide
        orders.window(Slide.over("10.rows").every("5.rows").on("rowtime").as("w")); //Count Slide


        // 1.3 会话Session窗口
        orders.window(Session.withGap("10.minutes").on("rowtime").as("w"));
        orders.window(Session.withGap("10.minutes").on("proctime").as("w"));


    }
}
