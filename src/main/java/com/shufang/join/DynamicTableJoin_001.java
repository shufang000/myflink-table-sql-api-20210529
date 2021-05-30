package com.shufang.join;

import com.shufang.utils.MyUtil;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class DynamicTableJoin_001 {
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
                        .field("o_rowtime", DataTypes.STRING())
                        .field("amount",DataTypes.INT())
                        .field("o_currency",DataTypes.STRING())
                ).createTemporaryTable("orders");
        Table orders = tableEnv.from("orders");


        //09:00,US Dollar,102
        tableEnv.connect(new FileSystem().path(path3))
                .withFormat(new Csv().ignoreParseErrors())
                .withSchema(new Schema()
                        .field("r_rowtime",DataTypes.STRING())
                        .field("r_currency",DataTypes.STRING())
                        .field("rate",DataTypes.DOUBLE())
                ).createTemporaryTable("rateshistory");
        Table rateshistory = tableEnv.from("rateshistory");

        /**
         * 全局join，状态很大，需要设置状态保留时间
         */
        tableEnv.getConfig().setIdleStateRetentionTime(Time.days(1),Time.days(2));
        Table select = orders.join(rateshistory).where("o_currency = r_currency").select("*");

        /**
         * 2 Time-Window Join
         */
        Table select1 = orders.join(rateshistory).where("o_currency = r_currency && o_rowtime >= r_rowtime").select("*");


        /**
         * 3 join Table Function
         */
        tableEnv.toAppendStream(select1, Row.class).print("join");


        streamEnv.execute();
    }
}
