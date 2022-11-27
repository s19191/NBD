package org.example;

import com.basho.riak.client.api.commands.kv.UpdateValue;

public class People {
    public String name;
    public String surname;
    public Integer age;
    public Boolean married;

    public People() {}

    public People(String name, String surname, Integer age, Boolean married) {
        this.name = name;
        this.surname = surname;
        this.age = age;
        this.married = married;
    }
}
