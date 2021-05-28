package com.shufang.partition;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RepartitionDirectDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStream<String> source = env.readTextFile("src/main/resources/temps.txt");


        source.shuffle(); //随机分配给下个oper的不同subtask

        source.broadcast(); //将数据完整的发送给下个oper的不同subtask

        source.rebalance();//轮询的发送

        source.rescale(); //先将subtask分组

        source.global(); //分配给下一个oper的第一个subtask实例

        source.keyBy(0); //按照hash分配

        source.forward(); //直接发给下一个oper的local task

        env.execute();
    }
}
