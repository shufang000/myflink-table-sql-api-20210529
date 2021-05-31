package com.shufang.hive;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

public class FlinkAndHiveIntegration {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "shufang");

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        StreamExecutionEnvironment stremEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(stremEnv, settings);


        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "src/main/resources"; // a local path
        String version = "1.2.2";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hive);

        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");


        System.out.println(tableEnv.getCurrentCatalog());
        System.out.println(tableEnv.getCurrentDatabase());


        /* *
         * emp
         * emp1
         * emp2
         * emp3
         * emp4
         * emp5
         * mykafka
         * */

/*        String[] strings = tableEnv.listTables();
        for (String string : strings) {
            System.out.println(string);
        }

        Table mykafka = tableEnv.sqlQuery("select * from mykafka");
        mykafka.printSchema();*/




        Table mytable = tableEnv.from("mytable");
        mytable.printSchema();



        tableEnv.sqlUpdate("insert into mytable values ('shufang',36.2)");

        tableEnv.execute("");

        //stremEnv.execute("");


    }
}
