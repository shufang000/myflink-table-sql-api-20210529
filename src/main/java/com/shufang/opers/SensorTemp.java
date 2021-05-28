package com.shufang.opers;

public class SensorTemp {
    public String id ;
    public Long timestamp;
    public Double temp;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemp() {
        return temp;
    }

    public void setTemp(Double temp) {
        this.temp = temp;
    }

    @Override
    public String toString() {
        return "SensorTemp{" +
                "id=" + id +
                ", timestamp=" + timestamp +
                ", temp=" + temp +
                '}';
    }

    public SensorTemp() {
    }

    public SensorTemp(String id, Long timestamp, Double temp) {
        this.id = id;
        this.timestamp = timestamp;
        this.temp = temp;
    }
}
