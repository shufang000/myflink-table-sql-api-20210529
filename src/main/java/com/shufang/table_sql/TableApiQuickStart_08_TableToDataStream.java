package com.shufang.table_sql;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
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


/**
 * 从Table到DataStream有2种转换形式
 * Append Mode  追加模式：这个模式仅仅当动态Table被Insert修改是才能使用，这是追加的形式，且之前操作的数据不作改变
 * Retract Mode  收回模式：这个模式始终可以使用
 *
 *  - add   新增数据
 *  - retract   撤回数据
 *  对于INSERT的数据，直接进行add操作 ，且flag=true
 *  对于DELETE的数据，直接进行retract操作，从流中撤回，且flag
 *  对于UPDATE的数据，首先将old的数据进行retract，然后将新的数据进行add
 * UPSERT Mode  更新插入模式：这里也是有2种操作
 * - upsert
 * - delete
 *  对于INSERT和UPDATE的数据，进行upsert的操作
 *  对于DELETE的数据，进行delete操作
 */
public class TableApiQuickStart_08_TableToDataStream {

    public static void main(String[] args) throws Exception {

        //0 创建一个Table API执行环境
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        StreamTableEnvironment tableAPIEnv = StreamTableEnvironment.create(streamEnv, setting);


        //1 创建一个Table
        tableAPIEnv.connect(new FileSystem().path("src/main/resources/sensor.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema().field("id",DataTypes.STRING()).field("temperture", DataTypes.DOUBLE()))
                .createTemporaryTable("sensor");
        Table sensor = tableAPIEnv.from("sensor");
        sensor.printSchema();

        Table sensorAgg = sensor.groupBy("id").select("id,id.count as ids,temperture.avg as tavg");

        //2 执行 Append Mode
        //tableAPIEnv.toAppendStream(sensor, Row.class).print();

        //3 执行 Retract Mode,这个通常在聚合操作之后使用
        //tableAPIEnv.toRetractStream(sensor,Row.class).print();
        tableAPIEnv.toRetractStream(sensorAgg,Row.class).print();


        streamEnv.execute();

    }
}
