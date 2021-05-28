package com.shufang.table_sql;


import com.shufang.beans.SensorTemper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;


/**
 * 单独使用Table&SQL API来Source Sink,而不是使用DataStream通过fromDataStream()来进行转换
 *
 * 在Flink中Table实际上由3个层级组成
 * - Catalog
 *      - Database
 *           -Identifier Object(表名)
 * 从这个结构上实际上与Presto查询引擎的内存结构层级是一样的，只不过presto是以下层级
 * - Catalog
 *      - Schema
 *          -Table(表名)
 * 在Flink中表可以是常规的，也可以是虚拟的(视图、View)
 * 通常通过connector连接的实体的表就是常规表(DataStream\Dataset\Mysql\Kafka....)，而在内存中临时生成的表结构就是虚拟的表，生命周期随着应用的结束而结束
 */
public class TableApiQuickStart_04_TableSourceFromFileGoodAgg {

    public static void main(String[] args) throws Exception {

        //0 创建一个Table API执行环境
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableAPIEnv = StreamTableEnvironment.create(streamEnv, setting);

        //1 创建一个SourceTable,连接外部系统创建一个表
        //tableAPIEnv.connect().createTemporaryTable("inputTable");

        tableAPIEnv.connect(new FileSystem().path("src/main/resources/sensor.txt"))
                .withFormat(new Csv())
                .withSchema(
                        new Schema()
                        .field("id", DataTypes.STRING())
                        .field("tempe", DataTypes.DOUBLE())
                ).createTemporaryTable("sensor");

        Table sensor = tableAPIEnv.from("sensor");

        //1 Table API实现简单的聚合和转化
        Table table1 = sensor
                .select("id,tempe")
                .where("id = 'sensor1'");
        tableAPIEnv.toAppendStream(table1,Row.class).print("table1");

        Table aggTable = sensor.groupBy("id")
                .select("id,id.count,tempe.avg as avg_tmp");
        tableAPIEnv.toRetractStream(aggTable,Row.class).print("aggTable");


        /**
         * aggTable:14> (true,sensor1,1,36.7)  true表示插入
         * aggTable:14> (false,sensor1,1,36.7) false表示撤回之前的
         * aggTable:14> (true,sensor1,2,33.45) true表示更新插入
         */
        //2 SQL API实现简单的转化聚合
        Table table = tableAPIEnv.sqlQuery("select id,tempe from sensor where id = 'sensor1'");
        tableAPIEnv.toAppendStream(table,Row.class).print();
        Table table2 = tableAPIEnv.sqlQuery("SELECT id,count(id) as ids ,avg(tempe) from sensor group by id");
        tableAPIEnv.toRetractStream(table2,Row.class).print("table2");

        System.out.println("-----------------------------------------------------");
        System.out.println(tableAPIEnv.getCurrentCatalog());
        System.out.println(tableAPIEnv.getCurrentDatabase());
        streamEnv.execute();
    }
}
