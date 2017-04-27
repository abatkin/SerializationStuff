package net.batkin.s11n.avro.deserializer;

import net.batkin.s11n.deserializer.Deserializer;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DeserializerWithSchema<T> implements Deserializer<T> {

    private Schema schema;
    private DatumReader<T> datumReader;

    public DeserializerWithSchema(Schema schema) {
        this.schema = schema;
        this.datumReader = new SpecificDatumReader<>(schema);
    }

    @Override
    public List<T> deserialize(byte[] bytes) throws IOException {
        List<T> items = new ArrayList<>();
        DataFileReader<T> dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(bytes), datumReader);
        while (dataFileReader.hasNext()) {
            T item = dataFileReader.next();
            items.add(item);
        }
        return items;
    }
}
