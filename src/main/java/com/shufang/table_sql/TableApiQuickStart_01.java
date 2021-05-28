package com.shufang.table_sql;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptor;

/**
 * 本类讲解如何使用JavaAPI调用Blink&Old planner创建对应的Table执行环境
 */
public class TableApiQuickStart_01 {
    public static void main(String[] args) {

        /*
         * 1.1 使用older planner接受流式数据源环境
         */
        //EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        //StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);


        /*
         * 1.2 使用older planner接受批次数据源环境
         */
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment benv = BatchTableEnvironment.create(env);


        /*
         * 2.1 使用blink planner构建流式数据源环境
         */
        //EnvironmentSettings envSetting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, envSetting);


        /*
         * 2.2 使用blink planner构建批次数据源环境
         */
        //EnvironmentSettings envSetting = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        //TableEnvironment tableEnv = TableEnvironment.create(envSetting);

    }
}
