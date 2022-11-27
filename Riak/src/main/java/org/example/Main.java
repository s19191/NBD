package org.example;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.UpdateValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;

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
            System.out.println("Me saved as String");

            People Mark = new People("Mark", "Kowalski", 99, true);
            Location markLocation = new Location(s19191Bucket, "mark");
            StoreValue storeMarkOp = new StoreValue.Builder(Mark)
                    .withLocation(markLocation)
                    .build();

            client.execute(storeMarkOp);
            System.out.println("Mark saved as object");

            Car Toyota = new Car("Mark", 2010, 7.6);
            Location toyotaLocation = new Location(s19191Bucket, "toyota");
            StoreValue storeToyotaOp = new StoreValue.Builder(Toyota)
                    .withLocation(toyotaLocation)
                    .build();

            client.execute(storeToyotaOp);
            System.out.println("Toyota saved as object");

            Location readKey = new Location(new Namespace("s19191"), "mark");
            FetchValue fetch = new FetchValue.Builder(readKey)
                    .withOption(FetchValue.Option.R, new Quorum(3))
                    .build();
            FetchValue.Response readResponse = client.execute(fetch);
            RiakObject readObj = readResponse.getValue(RiakObject.class);
            System.out.println(readObj.getValue());

            Mark.age = 50;
            PeopleUpdate updatedBook = new PeopleUpdate(Mark);
            UpdateValue updateValue = new UpdateValue.Builder(markLocation)
                    .withUpdate(updatedBook).build();
            UpdateValue.Response responseForUpdate = client.execute(updateValue);

            System.out.println("Mark updated");

            FetchValue.Response readResponseAfterUpdate = client.execute(fetch);
            RiakObject readObjAfterUpdate = readResponseAfterUpdate.getValue(RiakObject.class);
            System.out.println(readObjAfterUpdate.getValue());

            Location geniusQuote = new Location(new Namespace("s19191"), "mark");
            DeleteValue delete = new DeleteValue.Builder(geniusQuote).build();
            client.execute(delete);

            System.out.println("Mark deleted");

            FetchValue.Response readResponseAfterRemoval = client.execute(fetch);
            RiakObject readObjAfterRemoval = readResponseAfterRemoval.getValue(RiakObject.class);
            System.out.println(readObjAfterRemoval.getValue());

            cluster.shutdown();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}