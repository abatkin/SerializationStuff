package net.batkin.s11n.serializer;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class SerializerWithSchema<T> implements Serializer<T> {

    private Schema schema;
    private DatumWriter datumWriter;
    private DataFileWriter<T> writer;
    private ByteArrayOutputStream bos;
    private boolean isOpen;

    public SerializerWithSchema(Schema schema) throws IOException {
        this.schema = schema;
        this.datumWriter = new SpecificDatumWriter(schema);
        this.writer = new DataFileWriter<>(datumWriter);
        this.bos = new ByteArrayOutputStream();
    }

    @Override
    public void serialize(T item) throws IOException {
        if (!isOpen) {
            writer.create(schema, bos);
            isOpen = true;
        }
        writer.append(item);
    }

    @Override
    public byte[] getBytes() throws IOException {
        if (!isOpen) {
            throw new IOException("Serializer is not open");
        }
        writer.close();
        byte[] bytes = bos.toByteArray();
        bos.reset();
        isOpen = false;
        return bytes;
    }

}
