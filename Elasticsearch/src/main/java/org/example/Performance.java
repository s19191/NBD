package org.example;

import java.util.Date;

public class Performance {
    public Date timestamp;
    public String hostName;
    public String ipAddress;
    public Double CPU;
    public Double GPU;

    public Performance() {
    }

    public Performance(Date timestamp, String hostName, String ipAddress, Double CPU, Double GPU) {
        this.timestamp = timestamp;
        this.hostName = hostName;
        this.ipAddress = ipAddress;
        this.CPU = CPU;
        this.GPU = GPU;
    }

    @Override
    public String toString() {
        return "Performance{" +
                "timestamp=" + timestamp +
                ", hostName='" + hostName + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", CPU=" + CPU +
                ", GPU=" + GPU +
                '}';
    }
}
