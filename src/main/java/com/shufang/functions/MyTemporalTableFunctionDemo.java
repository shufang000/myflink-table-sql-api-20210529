package com.shufang.functions;

import com.mysql.cj.protocol.ResultsetRowsOwner;
import com.shufang.utils.MyUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;


/**
 * Note 这个仅仅支持 Inner Join
 * TableFunction 支持 Outer Join
 */
public class MyTemporalTableFunctionDemo {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, setting);
        String path2 = MyUtil.getPath("path2");
        String path3 = MyUtil.getPath("path3");

        System.out.printf("%s,%s\n", path2, path3);


        //10:15,2,Euro
        SingleOutputStreamOperator<Tuple3<String, Long, String>> orderStream = streamEnv.readTextFile(path2).map(new MapFunction<String, Tuple3<String, Long, String>>() {
            @Override
            public Tuple3<String, Long, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple3<>(split[0], Long.parseLong(split[1]), split[2]);
            }
        });
        Table orders = tableEnv.fromDataStream(orderStream, "o_time,amount,o_currency,o_proctime.proctime");
        orders.printSchema();


        //09:00,US Dollar,102
        SingleOutputStreamOperator<Tuple3<String, String, Double>> rateshistoryStream = streamEnv.readTextFile(path3).map(new MapFunction<String, Tuple3<String, String, Double>>() {

            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple3<>(split[0],split[1],Double.parseDouble(split[2]));
            }
        });
        Table rateshistory = tableEnv.fromDataStream(rateshistoryStream, "r_time,r_currency,rate,r_proctime.proctime");
        rateshistory.printSchema();


        TemporalTableFunction temporalTableFunction = rateshistory.createTemporalTableFunction("r_proctime", "r_currency");
        tableEnv.registerFunction("recentRate",temporalTableFunction);


        // 临时Table
        Table select = orders.joinLateral("recentRate(o_proctime)","o_currency = r_currency").select();

        /**
         *  join temporal table
         * temporal table func> (true,10:30,1,US Dollar,2021-05-30T13:11:42.661,09:00,US Dollar,102.0,2021-05-30T13:11:42.663)
         * temporal table func> (true,10:32,50,Yen,2021-05-30T13:11:42.663,09:00,Yen,1.0,2021-05-30T13:11:42.663)
         * temporal table func> (true,10:52,3,Euro,2021-05-30T13:11:42.663,10:45,Euro,116.0,2021-05-30T13:11:42.663)
         * temporal table func> (true,11:04,5,US Dollar,2021-05-30T13:11:42.663,09:00,US Dollar,102.0,2021-05-30T13:11:42.663)
         */
        tableEnv.toRetractStream(select, Row.class).print("temporal table func");


        streamEnv.execute("temporal table function");

    }
}
