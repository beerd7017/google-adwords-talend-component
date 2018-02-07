package com.adwords.google.desj;

public class MetricValue {
    public String name;
    public Double value;
    public int rowNum;

    public MetricValue() {
    }

    public String toString() {
        return "METRIC #" + this.rowNum + " " + this.name + "=" + this.value;
    }
}
