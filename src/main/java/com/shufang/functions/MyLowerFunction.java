package com.shufang.functions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;

public class MyLowerFunction extends ScalarFunction {
    public String eval(String a) {
        return a.toLowerCase();
    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.STRING;
    }


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        ArrayList<String> strings = new ArrayList<>();
        strings.add("A");
        strings.add("B");
        strings.add("C");

        DataStreamSource<String> source = streamEnv.fromCollection(strings);
        Table name = tableEnv.fromDataStream(source).as("name");

        // 注册一个Row Base的函数
        tableEnv.registerFunction("lowa",new MyLowerFunction());

        Table select = name.select("lowa(name)");
        tableEnv.toAppendStream(select, Row.class).print();


        streamEnv.execute();


    }
}
