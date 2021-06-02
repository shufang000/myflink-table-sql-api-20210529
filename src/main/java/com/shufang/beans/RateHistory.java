package com.shufang.beans;


/**
 * 11:49,Pounds,108
 */
public class RateHistory {
    public Long timeStamp;
    public String currency;
    public Double rate;


    @Override
    public String toString() {
        return "RateHistory{" +
                "timeStamp=" + timeStamp +
                ", currency='" + currency + '\'' +
                ", rate=" + rate +
                '}';
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public Double getRate() {
        return rate;
    }

    public void setRate(Double rate) {
        this.rate = rate;
    }

    public RateHistory() {
    }

    public RateHistory(Long timeStamp, String currency, Double rate) {

        this.timeStamp = timeStamp;
        this.currency = currency;
        this.rate = rate;
    }
}
