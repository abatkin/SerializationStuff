package net.batkin.s11n.avro.serializer;

import net.batkin.s11n.serializer.Serializer;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class SerializerWithoutSchema<T> implements Serializer<T> {

    private ByteArrayOutputStream bos;
    private DatumWriter datumWriter;
    private BinaryEncoder encoder;
    private Schema schema;
    private boolean isOpen;

    public SerializerWithoutSchema(Schema schema) {
        this.schema = schema;
        this.bos = new ByteArrayOutputStream();
        this.datumWriter = new SpecificDatumWriter(schema);
    }

    @Override
    public void serialize(T item) throws IOException {
        if (!isOpen) {
            encoder = EncoderFactory.get().binaryEncoder(bos, encoder);
            isOpen = true;
        }
        datumWriter.write(item, encoder);
    }

    @Override
    public byte[] getBytes() throws IOException {
        if (!isOpen) {
            throw new IOException("Serializer is not open");
        }
        encoder.flush();
        byte[] bytes = bos.toByteArray();
        bos.reset();
        isOpen = false;
        return bytes;
    }

}
