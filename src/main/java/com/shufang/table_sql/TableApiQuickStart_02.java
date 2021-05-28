package com.shufang.table_sql;


import com.shufang.beans.SensorTemper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 如何使用Java API完成以下过程
 * 1、注册一个StreamTable
 * 2、查询一个Table
 * 3、发射一个Table
 *
 * root
 *  |-- id: STRING
 *  |-- tempe: DOUBLE
 *
 * tableResult > sensor1,36.7
 * sqlResult > sensor1,36.7
 * tableResult > sensor1,34.1
 * sqlResult > sensor1,34.1
 * tableResult > sensor1,30.2
 * sqlResult > sensor1,30.2
 * sqlResult > sensor2,18.3
 * sqlResult > sensor2,36.1
 */
public class TableApiQuickStart_02 {
    public static void main(String[] args) throws Exception {

        // 1 创建执行环境，假设从文件创建一个表,如果不指定panner，默认使用OldPlanner

        EnvironmentSettings setting = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,setting);



        // 2 从文件创建一个DataStream
        DataStreamSource<String> fileStream = env.readTextFile("src/main/resources/sensor.txt");
        // 转换成POJO类型
        SingleOutputStreamOperator<SensorTemper> sensorStream = fileStream.map(new MapFunction<String, SensorTemper>() {
            @Override
            public SensorTemper map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorTemper(fields[0], new Double(fields[1]));
            }
        });


        // 将DataStream转换成一个Table，并完成注册
        Table table = tableEnv.fromDataStream(sensorStream);

        table.printSchema();

        // 3 查询一个表
        // 3.1 使用table api进行查询
        Table tableResult = table.select("id,tempe").where("id = 'sensor1'");
        tableEnv.createTemporaryView("sensor",table);
        String sql = "select id,tempe from sensor";
        Table sqlResult = tableEnv.sqlQuery(sql);


        // 4 分别打印不同的API的结果，首先转换成DataStream
        tableEnv.toAppendStream(tableResult, Row.class).print("tableResult ");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult ");


        // 5 最终使用env.execute()执行
        env.execute();
    }
}
