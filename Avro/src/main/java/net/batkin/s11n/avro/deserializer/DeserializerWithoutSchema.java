package net.batkin.s11n.avro.deserializer;

import net.batkin.s11n.deserializer.Deserializer;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DeserializerWithoutSchema<T> implements Deserializer<T> {

    private Schema schema;
    private DatumReader<T> datumReader;
    private BinaryDecoder decoder;

    public DeserializerWithoutSchema(Schema schema) {
        this.schema = schema;
        this.datumReader = new SpecificDatumReader<>(schema);
    }

    @Override
    public List<T> deserialize(byte[] bytes) throws IOException {
        List<T> items = new ArrayList<>();
        this.decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bytes), this.decoder);
        while (!decoder.isEnd()) {
            T item = datumReader.read(null, decoder);
            items.add(item);
        }
        return items;
    }
}
