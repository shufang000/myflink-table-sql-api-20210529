package com.shufang.table_sql;

import com.shufang.utils.MyUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class TableApiQuickStart_11_JOIN {
    public static void main(String[] args) throws Exception {

        EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, setting);
        String path2 = MyUtil.getPath("path2");
        String path3 = MyUtil.getPath("path3");

        System.out.printf("%s,%s\n",path2,path3);

        //10:15,2,Euro
        tableEnv.connect(new FileSystem().path(path2))
                .withFormat(new Csv().ignoreParseErrors())
                .withSchema(new Schema()
                        .field("rowtime",DataTypes.STRING())
                        .field("amount",DataTypes.INT())
                        .field("currency",DataTypes.STRING())
                        .field("proctime", DataTypes.TIMESTAMP(3)).proctime()
                ).createTemporaryTable("orders");
        tableEnv.from("orders").printSchema();


        //09:00,US Dollar,102
        tableEnv.connect(new FileSystem().path(path3))
                .withFormat(new Csv().ignoreParseErrors())
                .withSchema(new Schema()
                        .field("rowtime",DataTypes.STRING())
                        .field("currency",DataTypes.STRING())
                        .field("rate",DataTypes.DOUBLE())
                        .field("proctime", DataTypes.TIMESTAMP(3)).proctime()
                ).createTemporaryTable("rateshistory");
        tableEnv.from("rateshistory").printSchema();


        // temporal table Join
        String sqlQuery="select\n" +
                "  sum(o.amount * r.rate) as amount\n" +
                "from orders as o,\n" +
                "  rateshistory as r\n" +
                "where r.currency = o.currency\n" +
                "and r.rowtime = (\n" +
                "  select max(rowtime)\n" +
                "  from rateshistory as r2\n" +
                "  where r2.currency = o.currency\n" +
                "  and r2.rowtime <= o.rowtime)";
        Table table = tableEnv.sqlQuery(sqlQuery);

        tableEnv.toRetractStream(table,Row.class).print("join");


        streamEnv.execute();

    }
}
