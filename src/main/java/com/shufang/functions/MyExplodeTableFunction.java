package com.shufang.functions;


import com.shufang.utils.MyUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;

public class MyExplodeTableFunction extends TableFunction<String> {

    public void eval(String str) {
        if (str.contains("-")){
            String[] split = str.split("-");
            for (String s : split) {
                collector.collect(s);
            }
        }
    }

    @Override
    public TypeInformation<String> getResultType() {
        return Types.STRING;
    }

    public static void main(String[] args) throws Exception {

        String path5 = MyUtil.getPath("path5");


        ArrayList<String> strings = new ArrayList<>();
        strings.add("hello-word-this");
        strings.add("good-morning-hello");
        strings.add("fuck-off-man");


        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        DataStreamSource<String> stringStream = streamEnv.fromCollection(strings);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        Table names = tableEnv.fromDataStream(stringStream).as("names");
        names.printSchema();

        // 注册一个TableFunction
        tableEnv.registerFunction("myExplode",new MyExplodeTableFunction());
        Table select = names.joinLateral("myExplode(names) as name");

        select.printSchema();

        tableEnv.toAppendStream(select, Row.class).print("explode function is table function");

        stringStream.print();
        streamEnv.execute("fuck");

    }
}
