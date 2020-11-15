package com.happy3w.persistence.es.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.elasticsearch.common.transport.TransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class EsConnectConfig {
    private String hosts;
    private String clusterName;

    public List<TransportAddress> allTransportAddress() throws UnknownHostException {
        List<TransportAddress> addrs = new ArrayList<>();
        for (String host : hosts.split(",")) {
            String[] items = host.split(":");
            if (items.length != 2) {
                throw new IllegalArgumentException("Hosts must configured like IP:Port");
            }

            addrs.add(new TransportAddress(InetAddress.getByName(items[0]), Integer.parseInt(items[1])));
        }
        return addrs;
    }
}
