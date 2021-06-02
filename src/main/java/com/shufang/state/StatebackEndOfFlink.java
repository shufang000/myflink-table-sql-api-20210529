package com.shufang.state;


import com.shufang.beans.Orders;
import com.shufang.beans.RateHistory;
import com.shufang.functions.MyOrderSourceFunction;
import com.shufang.functions.MyRateSourceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * Flink本身支持3种不同的StateBackEnd
 * 1、MemoryStateBackEnd
 * -  主要是将状态保存在JobManager的内存中，平常可以用来测试
 * -  生产绝对不会使用的，因为状态不持久化到磁盘不可靠，如果状态过大还会OOM
 * -  不支持增量的checkpoint
 * 2、FSStateBackEnd
 * - 主要是将状态存储在文件系统，如HDFS
 * - 不支持增量的checkpoint
 * 3、RocksDBBackEnd
 * - 是一个嵌入式的KV数据库，能实现快速读写
 * - 状态数据存储在本地磁盘
 * - 能够实现海量的状态数据的存储，且能实现快速的incremental checkpoint
 */
public class StatebackEndOfFlink {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //配置状态后端
        env.setStateBackend(
                new FsStateBackend("file:///D:/idea_projects/flink-demo-project-20210501/src/main/resources"));

        // order stream
        SingleOutputStreamOperator<Orders> ordersDataStream = env.addSource(
                new MyOrderSourceFunction()
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Orders>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Orders element) {
                return element.getTimestamp();
            }
        });
        SingleOutputStreamOperator<RateHistory> rateHistoryDataStream = env.addSource(new MyRateSourceFunction()).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<RateHistory>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(RateHistory element) {
                return element.getTimeStamp();
            }
        });

        // window join
        /*DataStream<String> windowJoinStream = ordersDataStream.join(rateHistoryDataStream)
                .where(Orders::getCurrency)
                .equalTo(RateHistory::getCurrency)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<Orders, RateHistory, String>() {
                    @Override
                    public String join(Orders first, RateHistory second) throws Exception {
                        return first.toString() + ">>>>>>>" + second.toString();
                    }
                });
         */

        // interval join

        SingleOutputStreamOperator<String> intervalStream = ordersDataStream.keyBy("currency")
                .intervalJoin(rateHistoryDataStream.keyBy("currency"))
                .between(Time.seconds(-3), Time.seconds(2))
                .process(new ProcessJoinFunction<Orders, RateHistory, String>() {
                    @Override
                    public void processElement(Orders left, RateHistory right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(left.toString() + "---->" + right.toString());
                    }
                });


        intervalStream.print("event time =>");


        env.execute();
    }
}
