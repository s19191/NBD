package org.example;

import redis.clients.jedis.Jedis;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        try {
            Jedis jedis = new Jedis("localhost", 6379);
            System.out.println("Connection to server sucessfully");

            jedis.ltrim("list", -1, 0);

            Scanner scanner = new Scanner(System.in);
            int counter = 0;
            String s1 = "";

            System.out.println("Wprowadź dane do listy:");
            while (!s1.equals("exit") && scanner.hasNext()) {
                s1 = scanner.next();
                if (!s1.equals("exit")) {
                    System.out.println(counter);
                    if (counter == 4) {
                        jedis.lpush("list", s1);
                        System.out.println(jedis.lrange("list", 0, -1));
                    } else if (counter >= 5) {
                        jedis.rpop("list");
                        jedis.lpush("list", s1);
                        System.out.println(jedis.lrange("list", 0, -1));
                    } else {
                        jedis.lpush("list", s1);
                    }
                    counter++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}