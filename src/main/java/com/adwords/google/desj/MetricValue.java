package com.adwords.google.desj;

public class MetricValue {
    public String name;
    public Double value;
    public int rowNum;

    public MetricValue() {
    }

    public String toString() {
        return "METRIC #" + rowNum + " " + name + "=" + value;
    }
}
