package com.adwords.google.desj;

public class DimensionValue {
    public String name;
    public String value;
    public int rowNum;

    public DimensionValue() {
    }

    public String toString() {
        return "DIM #" + rowNum + " " + name + "=" + value;
    }
}

