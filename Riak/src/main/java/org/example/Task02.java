package org.example;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.indexes.BinIndexQuery;
import com.basho.riak.client.api.commands.indexes.IntIndexQuery;
import com.basho.riak.client.api.commands.indexes.SecondaryIndexQuery;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.indexes.StringBinIndex;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Task02 {
    public static void main(String[] args) {
        RiakCluster cluster = null;
        try {
            cluster = RiakConnectionHelper.setUpCluster();
            RiakClient client = new RiakClient(cluster);

            Namespace part2Bucket = new Namespace("part2");


//            BinIndexQuery biq = new BinIndexQuery.Builder(part2Bucket, "person_age", "18")
//                    .build();
//            BinIndexQuery.Response response = client.execute(biq);

            IntIndexQuery iiq = new IntIndexQuery.Builder(part2Bucket, "person_age", 18L)
                    .build();
            IntIndexQuery.Response response = client.execute(iiq);

            Map<BinaryValue, RiakObject> peopleAge18 = new HashMap();
            List<SecondaryIndexQuery.Response.Entry<Long>> entries = response.getEntries();
            for (BinIndexQuery.Response.Entry entry : entries) {
                Location readKey = new Location(new Namespace("part2"), entry.getRiakObjectLocation().getKey());
                FetchValue fetch = new FetchValue.Builder(readKey)
                        .withOption(FetchValue.Option.R, new Quorum(3))
                        .build();
                FetchValue.Response readResponse = client.execute(fetch);
                RiakObject readObj = readResponse.getValue(RiakObject.class);
                peopleAge18.put(entry.getRiakObjectLocation().getKey(), readObj);
                System.out.println(readObj.getValue());
                System.out.println(readObj.getValue().toString().split(",")[0].split("\"")[3] + ", " + readObj.getValue().toString().split(",")[1].split("\"")[3]);
            }

            peopleAge18.forEach((key, person) -> {
                person.getIndexes().getIndex(StringBinIndex.named("person_status")).add("dorosly");
                Location personLocation = new Location(new Namespace("part2"), key);

                StoreValue store = new StoreValue.Builder(person)
                        .withLocation(personLocation)
                        .build();
                try {
                    client.execute(store);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });

            cluster.shutdown();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            cluster.shutdown();
        }
    }
}
