package com.shufang.beans;


/**
 * 10:15,2,Euro
 * 这是一个Order的类
 */
public class Orders {
    public Long timestamp;
    public Long amount;
    public String currency;

    @Override
    public String toString() {
        return "Orders{" +
                "timestamp=" + timestamp +
                ", amount=" + amount +
                ", currency='" + currency + '\'' +
                '}';
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getAmount() {
        return amount;
    }

    public void setAmount(Long amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public Orders() {
    }

    public Orders(Long timestamp, Long amount, String currency) {
        this.timestamp = timestamp;
        this.amount = amount;
        this.currency = currency;
    }
}
