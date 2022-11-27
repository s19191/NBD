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

            /**
             * Kropka pierwsza oraz druga, wyszukiwanie ludzi o wieku równym 18.
             */

            IntIndexQuery iiq_person_age_18 = new IntIndexQuery.Builder(part2Bucket, "person_age", 18L)
                    .build();
            IntIndexQuery.Response response_person_age_18 = client.execute(iiq_person_age_18);

            /**
             * Mapa przechowywująca wszystkie te osoby.
             */

            Map<BinaryValue, RiakObject> peopleAge18 = new HashMap();

            List<SecondaryIndexQuery.Response.Entry<Long>> response_person_age_18Entries = response_person_age_18.getEntries();

            /**
             * Wyszukiwanie osób po ich kluczu, dodanie do hashmapy (klucz, osoba) oraz wypisanie ich imion oraz nazwisk.
             */

            for (BinIndexQuery.Response.Entry entry : response_person_age_18Entries) {
                Location readKey = new Location(new Namespace("part2"), entry.getRiakObjectLocation().getKey());
                FetchValue fetch = new FetchValue.Builder(readKey)
                        .withOption(FetchValue.Option.R, new Quorum(3))
                        .build();
                FetchValue.Response readResponse = client.execute(fetch);
                RiakObject readObj = readResponse.getValue(RiakObject.class);
                peopleAge18.put(entry.getRiakObjectLocation().getKey(), readObj);
                System.out.println(readObj.getValue().toString().split(",")[0].split("\"")[3] + ", " + readObj.getValue().toString().split(",")[1].split("\"")[3]);
            }

            /**
             * Kropka trzecia, na podstawie hashmapy iterujemy po każdej osobie oraz dodajemy jej index person_status - "dorosly". Na sam koniec zapisujemy, aby index pojawił się w bazie.
             */

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

            /**
             * Kropka czwarta, wyszukiwanie ludzi o wieku 65, bądź większym.
             */

            IntIndexQuery iiq_person_age_Above65 = new IntIndexQuery.Builder(part2Bucket, "person_age", 65L, Long.MAX_VALUE)
                    .build();
            IntIndexQuery.Response response_person_age_Above65 = client.execute(iiq_person_age_Above65);

            List<SecondaryIndexQuery.Response.Entry<Long>> response_person_age_above65Entries = response_person_age_Above65.getEntries();

            /**
             * Po kluczu znalezionych osób je pobieramy. Następnie dodajemy im nowy index person_status o wartości "emeryt" oraz ponownie zapisujemy.
             */

            for (BinIndexQuery.Response.Entry entry : response_person_age_above65Entries) {
                Location readKey = new Location(new Namespace("part2"), entry.getRiakObjectLocation().getKey());
                FetchValue fetch = new FetchValue.Builder(readKey)
                        .withOption(FetchValue.Option.R, new Quorum(3))
                        .build();
                FetchValue.Response readResponse = client.execute(fetch);

                RiakObject pensioner = readResponse.getValue(RiakObject.class);
                pensioner.getIndexes().getIndex(StringBinIndex.named("person_status")).add("emeryt");

                StoreValue store = new StoreValue.Builder(pensioner)
                        .withLocation(readKey)
                        .build();
                try {
                    client.execute(store);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            /**
             * Kropka piąta, wyszukiwanie ludzi o indexie person_status - emeryt.
             */

            BinIndexQuery biq = new BinIndexQuery.Builder(part2Bucket, "person_status", "emeryt")
                    .build();
            BinIndexQuery.Response response = client.execute(biq);
            List<SecondaryIndexQuery.Response.Entry<String>> entries = response.getEntries();

            /**
             * Wyświetlanie wszystkich emerytów w formacie klucz, wartość.
             */

            for (BinIndexQuery.Response.Entry entry : entries) {
                Location readKey = new Location(new Namespace("part2"), entry.getRiakObjectLocation().getKey());
                FetchValue fetch = new FetchValue.Builder(readKey)
                        .withOption(FetchValue.Option.R, new Quorum(3))
                        .build();
                FetchValue.Response readResponse = client.execute(fetch);
                RiakObject readObj = readResponse.getValue(RiakObject.class);
                System.out.println(readObj);
                System.out.println("Key: " + readKey.getKey() + ", value: " + readObj.getValue());
            }

            /**
             * Wyświetlanie liczby wszystkich emerytów.
             */

            System.out.println("Number of pensioners in database: " + entries.size());

            cluster.shutdown();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            cluster.shutdown();
        }
    }
}