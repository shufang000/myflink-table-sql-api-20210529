package com.shufang.table_sql;



import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.Properties;


/**
 * 目前已经全部调试成功
 */
public class TableApiQuickStart_06_TableSourceFromKafka {
    public static void main(String[] args) throws Exception {


        //1 创建环境
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,setting);

        // 这个需要在Flink的配置文件中进行配置，否则： A catalog with name [ShuFang's Catalog] does not exist.
        // tableEnv.useCatalog("ShuFang's Catalog");
        // tableEnv.useDatabase("ShuFang's Database");

        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", "shufang101:2181");
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "shufang101:9092");
        //"0.8", "0.9", "0.10", "0.11", and "universal",只能选中这几个，如果使用的是kafka1.0.0以上的版本，最好使用universal
        tableEnv.connect(
            new Kafka()
            .version("universal")
            .topic("sensor")
            .properties(properties)
        ).withFormat(new Csv())
         .withSchema(new Schema().field("id", DataTypes.STRING()).field("tempe", DataTypes.DOUBLE()))
         .createTemporaryTable("kafka_sensor");

        Table kafka_sensor = tableEnv.from("kafka_sensor");

        // 2 转换聚合
        Table trans = tableEnv.sqlQuery("select id,tempe from kafka_sensor ");
        //Table agg = tableEnv.sqlQuery("select id,avg(tempe) as tp_avg,count(id) as ids from kafka_sensor group by id ");

        // 3 打印输出,当然也可以输出到kafka的其它的topic
        tableEnv.toAppendStream(trans, Row.class).print("trans ===== > ");
        //tableEnv.toRetractStream(agg,Row.class).print("agg ==== >");

        // 3.1 往kafka的另一个topic sensor_sink中写入
        tableEnv.connect(
                new Kafka()
                .version("universal")
                .topic("sensor_sink")
                .properties(properties)
        ).withFormat(new Csv())
         .withSchema(new Schema().field("id", DataTypes.STRING()).field("tempe", DataTypes.DOUBLE()))
         .createTemporaryTable("sensor_sink");

        Table sensor_sink = tableEnv.from("sensor_sink");


        trans.insertInto("sensor_sink");


        // -1 开始执行
        env.execute("kafka");


    }
}
