package com.shufang.table_sql;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;


/**
 *  Table的操作实际上与流的操作是一致的
 *  1 Source 注册输入表
 *  2 Transform 进行转换
 *  3 Sink 注册输出表
 *  下面操作以下从文件读取数据，然后再将文件写入到文件
 */
public class TableApiQuickStart_05_TableSinkToFile {

    public static void main(String[] args) throws Exception {

        //0 创建一个Table API执行环境
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        StreamTableEnvironment tableAPIEnv = StreamTableEnvironment.create(streamEnv, setting);

        //1 注册输入表
        tableAPIEnv.connect(new FileSystem().path("src/main/resources/sensor.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema().field("id", DataTypes.STRING()).field("tempe",DataTypes.DOUBLE()))
                .createTemporaryTable("sensor");
        Table source = tableAPIEnv.from("sensor");

        //2 注册输出表
        tableAPIEnv.connect(new FileSystem().path("src/main/resources/sensorOut.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("tempe",DataTypes.DOUBLE())
                ).createTemporaryTable("outputSensor");
        Table outputSensor = tableAPIEnv.from("outputSensor");

        //3 插入数据
        //source.insertInto("outputSensor");

        //4 聚合之后插入会报错：TableException: AppendStreamTableSink requires that Table has only insert changes.
        Table output = tableAPIEnv.sqlQuery("select id,count(id) ids,avg(tempe) as aTemp from sensor group by id");
        // emit 方式一：
        // output.insertInto("outputSensor");
        // emit 方式二：
        tableAPIEnv.sqlUpdate("INSERT INTO outputSensor SELECT id,tempe from sensor ");

        tableAPIEnv.execute("stream life man :");

    }
}
