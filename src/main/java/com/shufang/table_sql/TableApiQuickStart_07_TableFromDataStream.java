package com.shufang.table_sql;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import scala.Tuple12;


/**
 *
 */
public class TableApiQuickStart_07_TableFromDataStream {

    public static void main(String[] args) throws Exception {

        //0 创建一个Table API执行环境
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        StreamTableEnvironment tableAPIEnv = StreamTableEnvironment.create(streamEnv, setting);


        //1 创建一个Stream
        DataStreamSource<String> stream = streamEnv.readTextFile("src/main/resources/sensor.txt");
        SingleOutputStreamOperator<Tuple2<String, String>> tupleStream = stream.map(
                new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new Tuple2<>(split[0],split[1]);
                    }
                }
        );

        //开始从DataStream获取Table，同时可以指定你需要的Schema fields:"name1,name2"

        Table table = tableAPIEnv.fromDataStream(tupleStream);
        /**printSchema
         * root
         *  |-- f0: STRING
         *  |-- f1: STRING
         */
        table.printSchema();

        /**
         * root
         *  |-- name1: STRING
         *  |-- name2: STRING
         */
        Table table3 = tableAPIEnv.fromDataStream(tupleStream, "name1,name2");
        table3.printSchema();


        /**
         * root
         *  |-- name1: STRING
         */
        Table table4 = tableAPIEnv.fromDataStream(tupleStream, "name1");
        table4.printSchema();

        /**
         * sensor1
         * sensor1
         * sensor1
         * sensor2
         * sensor2
         */
        tableAPIEnv.toAppendStream(table4,String.class).print();


        System.out.println("===============================================================================");

        /**printSchema
         * root
         *  |-- name1: STRING
         *  |-- name2: STRING
         */
        tableAPIEnv.createTemporaryView("table1",tupleStream,"name1,name2");
        Table table1 = tableAPIEnv.sqlQuery("select * from table1");
        table1.printSchema();
        /**printSchema
         *  root
         *   |-- f0: STRING
         *   |-- f1: STRING
         */
        tableAPIEnv.createTemporaryView("table2",tupleStream);
        Table table2 = tableAPIEnv.sqlQuery("select * from table2");
        table2.printSchema();


        //2 执行
        streamEnv.execute();


        //TODO 与外部系统的数据更新模式 ：Append(文件、Kafka等消息队列)、Retract、Upsert(ES\MySQL)
        //Flink SQL使用的connect的描述器有很多：ElasticSearch、Kafka、FileSystem
    }
}
