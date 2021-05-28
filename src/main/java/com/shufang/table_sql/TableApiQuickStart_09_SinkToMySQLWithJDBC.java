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


public class TableApiQuickStart_09_SinkToMySQLWithJDBC {
    public static void main(String[] args) throws Exception {

        // 1 创建执行环境，假设从文件创建一个表,如果不指定panner，默认使用OldPlanner

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);



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
        Table table = tableEnv.fromDataStream(sensorStream,"id,tempe");
        table.printSchema();

        // 3 注册一个Mysql的sink表
        String sinkDDL = "CREATE TABLE sensor_count (\n" +
                "id varchar(20),\n" +
                "temper double\n" +
                ") WITH (\n" +
                "'connector.type' = 'jdbc',\n" +
                "'connector.url' = 'jdbc:mysql://shufang101:3306/hello', \n" +
                "'connector.table' = 'sensor_count', \n" +
                "'connector.driver' = 'com.mysql.cj.jdbc.Driver', \n" +
                "'connector.username' = 'root',\n" +
                "'connector.password' = '888888'\n" +
                ")";

        tableEnv.sqlUpdate(sinkDDL);

        table.insertInto("sensor_count");

        // 5 最终使用env.execute()执行
        env.execute();
    }
}
