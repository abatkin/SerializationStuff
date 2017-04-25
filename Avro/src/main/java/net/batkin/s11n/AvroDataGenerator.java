package net.batkin.s11n;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

public interface AvroDataGenerator<T> {

    T generateDatum(int id);

    default List<T> generateDataSeries(int num) {
        List<T> items = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            items.add(generateDatum(i));
        }
        return items;
    }

    Schema getSchema();
}
