package com.shufang.sink;

import com.shufang.beans.SensorTemper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecCalc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

public class MySQLSinkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStreamSource<String> source = env.readTextFile("src/main/resources/sensor.txt");

        DataStreamSource<String> source = env.socketTextStream("shufang101", 9999);
        SingleOutputStreamOperator<SensorTemper> sensorTemperStream = source.map(new MapFunction<String, SensorTemper>() {
            @Override
            public SensorTemper map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorTemper(fields[0], new Double(fields[1]));
            }
        });

        sensorTemperStream.addSink(new MyJDBCSink());


        env.execute();
    }

    private static class MyJDBCSink extends RichSinkFunction<SensorTemper> {
        static Properties properties = new Properties();

        static {
            try {
                properties.load(MyJDBCSink.class.getClassLoader().getResourceAsStream("jdbc.properties"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        Connection connection = null;
        PreparedStatement insertState = null;
        PreparedStatement updateState = null;

        //进行初始化
        @Override
        public void open(Configuration parameters) throws Exception {
            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            String driver = properties.getProperty("driver");

            Class.forName(driver);
            connection = DriverManager.getConnection(url, username, password);

            insertState = connection.prepareStatement("insert into sensor_temper(id ,temper) values(?,?)");
            updateState = connection.prepareStatement("update sensor_temper set temper = ? where id = ?");
        }

        @Override
        public void invoke(SensorTemper value, Context context) throws Exception {

            updateState.setDouble(1,value.getTempe());
            updateState.setString(2,value.getId());
            updateState.execute();

            if (updateState.getUpdateCount() == 0){
                insertState.setString(1,value.getId());
                insertState.setDouble(2,value.getTempe());
                insertState.execute();
            }

            super.invoke(value, context);
        }

        @Override
        public void close() throws Exception {
            updateState.close();
            insertState.close();
            connection.close();
        }
    }
}
