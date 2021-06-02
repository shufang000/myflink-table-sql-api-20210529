package com.shufang.functions;

import com.shufang.beans.Orders;
import com.shufang.beans.RateHistory;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class MyRateSourceFunction implements SourceFunction<RateHistory> {
    @Override
    public void run(SourceContext<RateHistory> ctx) throws Exception {
        while (true) {
            ctx.collect(getRateHistory());
        }
    }

    @Override
    public void cancel() {

    }


    //11:49,Pounds,108
    public RateHistory getRateHistory() throws InterruptedException {
        String currencys = "Euro,US Dollar,Yen,Pounds";
        String[] currencyList = currencys.split(",");
        Random random = new Random();
        String currency = currencyList[random.nextInt(currencyList.length)];
        Long timestamp = System.currentTimeMillis() - random.nextInt(2) * 1000L;
        Double rate = 1D;

        /**
         * 09:00,US Dollar,102
         * 09:00,Euro,114
         * 09:00,Yen,1
         * 10:45,Euro,116
         * 11:15,Euro,119
         * 11:49,Pounds,108
         */
        switch (currency) {
            case "Euro":
                rate = rate + 101 + random.nextInt(10);
                break;
            case "Yen":
                rate = 1D;
                break;
            case "US Dollar":
                rate = rate + 90 + random.nextInt(5);
                break;
            case "Pounds":
                rate = rate + 70 + random.nextInt(20);
                break;
            default:
                rate = 2D;
        }

        Thread.sleep(5000);
        return new RateHistory(timestamp, currency, rate);

    }


}
