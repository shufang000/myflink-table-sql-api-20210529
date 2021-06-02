package com.shufang.functions;

import com.shufang.beans.Orders;
import com.shufang.beans.RateHistory;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Test;

import java.util.Random;

public class MyOrderSourceFunction implements SourceFunction<Orders> {
    @Override
    public void run(SourceContext ctx) throws Exception {
        while (true){
            ctx.collect(getOrderEvent());
        }
    }


    /**
     * 10:15,2,Euro
     * 10:30,1,US Dollar
     * 10:32,50,Yen
     * 10:52,3,Euro
     * 11:04,5,US Dollar
     *
     * @return
     */
    public Orders getOrderEvent() throws InterruptedException {
        String currencys = "Euro,US Dollar,Yen,Pounds";
        String[] currencyList = currencys.split(",");
        Random random = new Random();
        String currency = currencyList[random.nextInt(currencyList.length)];
        Long timestamp = System.currentTimeMillis() - random.nextInt(2) * 1000L;
        long amount = random.nextInt(20) + 2;
        Thread.sleep(1000);
        return new Orders(timestamp,amount,currency);
    }




    @Override
    public void cancel() {

    }


}
