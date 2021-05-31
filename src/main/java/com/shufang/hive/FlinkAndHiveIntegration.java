package com.shufang.hive;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

public class FlinkAndHiveIntegration {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment stremEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //TableEnvironment tableEnv = TableEnvironment.create(settings);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(stremEnv, settings);



        String name            = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir     = "src/main/resources"; // a local path
        String version         = "1.2.2";

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
         * mykafka*/

        String[] strings = tableEnv.listTables();
        for (String string : strings) {
            System.out.println(string);
        }

        Table table = tableEnv.sqlQuery("select * from emp1");



        table.printSchema();


        /**
         * 18> 9999,shufang
         * 19> 8888,lanyage
         * 1> 8888,yangtao
         * 20> 8888,yangtao
         * 7> 1001,su
         * 1> 1002,batman
         * 24> 1001,superman
         * 3> 8888,lanyage
         * 2> 9999,shufang
         */
        tableEnv.toAppendStream(table, Row.class).print();

        stremEnv.execute("");



    }
}
