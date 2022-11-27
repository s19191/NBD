package org.example;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.UpdateValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;

public class Task02 {
    public static void main(String[] args) {
        RiakCluster cluster = null;
        try {
            cluster = RiakConnectionHelper.setUpCluster();
            RiakClient client = new RiakClient(cluster);

            Namespace part2Bucket = new Namespace("part2");

            cluster.shutdown();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            cluster.shutdown();
        } finally {
            cluster.shutdown();
        }
    }
}
