package org.example;

import com.basho.riak.client.api.commands.kv.UpdateValue;

public class PeopleUpdate extends UpdateValue.Update<People> {
    private final People update;

    public PeopleUpdate(People update){
        this.update = update;
    }

    @Override
    public People apply(People t) {
        if(t == null) {
            t = new People();
        }

        t.name = update.name;
        t.surname = update.surname;
        t.age = update.age;
        t.married = update.married;

        return t;
    }
}