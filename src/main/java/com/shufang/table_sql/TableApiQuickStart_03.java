package com.shufang.table_sql;


import com.shufang.beans.SensorTemper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 如何使用Java API完成以下过程
 * 1、注册一个BatchTable
 * 2、查询一个Table
 * 3、发射一个Table
 *
 * root
 *  |-- id: STRING
 *  |-- tempe: DOUBLE
 * sensor1,34.1
 * sensor1,30.2
 * sensor1,36.7
 * ------------
 * sensor2,18.3
 * sensor1,30.2
 * sensor1,36.7
 * sensor2,36.1
 * sensor1,34.1
 */
public class TableApiQuickStart_03 {
    public static void main(String[] args) throws Exception {

        // 1 创建执行环境，假设从文件创建一个表,如果不指定panner，默认使用OldPlanner

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);



        // 2 从文件创建一个DataStream
        DataSource<String> dataSet = env.readTextFile("src/main/resources/sensor.txt");
        // 转换成POJO类型

        DataSet<SensorTemper> sensorSet = dataSet.map(new MapFunction<String, SensorTemper>() {
            @Override
            public SensorTemper map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorTemper(fields[0], new Double(fields[1]));
            }
        });

        // 将DataSet转换成一个Table，并完成注册
        Table table = tableEnv.fromDataSet(sensorSet);


        table.printSchema();

        // 3 查询一个表
        // 3.1 使用table api进行查询
        Table tableResult = table.select("id,tempe").where("id = 'sensor1'");
        tableEnv.createTemporaryView("sensor",table);
        String sql = "select id,tempe from sensor";
        Table sqlResult = tableEnv.sqlQuery(sql);


        // 4 分别打印不同的API的结果，首先转换成DataSet

        tableEnv.toDataSet(tableResult,Row.class).print();
        System.out.println("------------------------------");
        tableEnv.toDataSet(sqlResult,Row.class).print();



    }
}
