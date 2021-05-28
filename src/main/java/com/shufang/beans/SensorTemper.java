package com.shufang.beans;

public class SensorTemper {
    private String id ;
    private Double tempe;

    @Override
    public String toString() {
        return "SensorTemper{" +
                "id='" + id + '\'' +
                ", tempe=" + tempe +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Double getTempe() {
        return tempe;
    }

    public void setTempe(Double tempe) {
        this.tempe = tempe;
    }

    public SensorTemper() {
    }

    public SensorTemper(String id, Double tempe) {
        this.id = id;
        this.tempe = tempe;
    }
}
