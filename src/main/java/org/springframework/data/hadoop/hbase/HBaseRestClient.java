package org.springframework.data.hadoop.hbase;

 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.springframework.util.Assert;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


public class HBaseRestClient{
    /**
     * Thread safe HTTP Client for Hbase
     */
    private final Client client;

    public HBaseRestClient(String addresses) {
        Assert.notNull(addresses, " remote HBase address is required");        
        final Cluster cluster = createHBaseRemoteCluster( new ArrayList<String>(Arrays.asList(addresses.split(","))));
        client = new Client(cluster);

    }

    private Cluster createHBaseRemoteCluster(List<String> addresses) {
        final Cluster cluster = new Cluster();
        for (String address : addresses) {

            cluster.add(address);
        }
        return cluster;
    }


    public HTableInterface getHTable(String tableName, Configuration configuration, Charset charset, HTableInterfaceFactory tableFactory) {
        try {
            return new RemoteHTable(client, tableName);
        } catch (Exception e) {
            throw HbaseUtils.convertHbaseException(e);
        }
    }

    
    public void releaseTable(String tableName, HTableInterface table, HTableInterfaceFactory tableFactory) {
        /**
         * NO OP operation as calling table.close() on RemoteHTable shuts down underlying http client.
         */
    }

    public void shutdown() {
        client.shutdown();
    }

}