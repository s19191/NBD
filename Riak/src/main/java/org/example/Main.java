package org.example;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import java.net.UnknownHostException;

public class Main {

    private static RiakCluster setUpCluster() {
        // This example will use only one node listening on localhost:10017
        RiakNode node = new RiakNode.Builder()
                .withRemoteAddress("127.0.0.1")
                .withRemotePort(8087)
                .build();

        // This cluster object takes our one node as an argument
        RiakCluster cluster = new RiakCluster.Builder(node)
                .build();

        // The cluster must be started to work, otherwise you will see errors
        cluster.start();

        return cluster;
    }
    public static void main(String[] args) {
        try {
            RiakCluster cluster = setUpCluster();
            RiakClient client = new RiakClient(cluster);

            Namespace s19191Bucket = new Namespace("s19191");
            Location meLocation = new Location(s19191Bucket, "me");
            BinaryValue text = BinaryValue.create("{\n" +
                    "    \"name\" : \"Jan\",\n" +
                    "    \"surname\" : \"Kwasowski\",\n" +
                    "    \"age\" : 24,\n" +
                    "    \"married\" : false,\n" +
                    "    \"weight\" : 80\n" +
                    "}");
            RiakObject rufusObject = new RiakObject()
                    .setContentType("application/json")
                    .setValue(text);
            StoreValue storeMeOp = new StoreValue.Builder(rufusObject)
                    .withLocation(meLocation)
                    .build();

            client.execute(storeMeOp);

//            Location geniusQuote = new Location(new Namespace("s19191"), "me");
//            DeleteValue delete = new DeleteValue.Builder(geniusQuote).build();
//            client.execute(delete);

            cluster.shutdown();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        System.out.println("Hello world!");
    }
}