package net.batkin.s11n;

import net.batkin.s11n.avro.generated.AvroOrder;
import net.batkin.s11n.data.BenchmarkRunner;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static net.batkin.s11n.data.BenchmarkRun.r;
import static net.batkin.s11n.data.DataGenerator.NUM_ORDERS;
import static net.batkin.s11n.data.DataGenerator.NUM_RUNS;
import static net.batkin.s11n.data.Util.sumArrayLengths;

public class AvroSerializer<T> {

    private static final String OPERATION_SERIALIZE = "Serialize";

    public static void main(String[] args) throws Exception {
        BenchmarkRunner runner = new BenchmarkRunner(NUM_RUNS);
        SimpleOrderDataGenerator generator = new SimpleOrderDataGenerator();
        AvroSerializer<AvroOrder> serializer = new AvroSerializer<>(generator);
        serializer.runBenchmarks(NUM_ORDERS, runner);
        runner.dumpCsv(System.out);
    }

    public AvroSerializer(AvroDataGenerator<T> dataGenerator) {
        this.dataGenerator = dataGenerator;
    }

    private AvroDataGenerator<T> dataGenerator;

    public void runBenchmarks(int numItems, BenchmarkRunner runner) {
        List<T> items = dataGenerator.generateDataSeries(numItems);
        System.out.println("Generated items for Serializer");
        runner.runBenchmarks(
                r("With Schema, One Byte Array", OPERATION_SERIALIZE, items.size(), () -> benchmarkSerializeOneByteArrayWithSchema(items)),
                r("Without Schema, One Byte Array", OPERATION_SERIALIZE, items.size(), () -> benchmarkSerializeOneByteArrayWithoutSchema(items)),
                r("With Schema, Many Byte Arrays, New Serializer", OPERATION_SERIALIZE, items.size(), () -> serializeManyByteArraysWithSchemaNewSerializer(items)),
                r("Without Schema, Many Byte Arrays, New Serializer", OPERATION_SERIALIZE, items.size(), () -> serializeManyByteArraysWithoutSchemaNewSerializer(items)),
                r("With Schema, Many Byte Arrays, Reuse Serializer", OPERATION_SERIALIZE, items.size(), () -> benchmarkSerializeManyByteArraysWithSchemaReuseSerializer(items)),
                r("Without Schema, Many Byte Arrays, Reuse Serializer", OPERATION_SERIALIZE, items.size(), () -> benchmarkSerializeManyByteArraysWithoutSchemaReuseSerializer(items))
        );
    }

    public static <T> byte[] serializeOneByteArrayWithSchema(Schema schema, Collection<T> items) {
        DatumWriter datumWriter = new SpecificDatumWriter(schema);
        DataFileWriter<T> writer = new DataFileWriter<>(datumWriter);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            writer.create(schema, bos);
            for (T item : items) {
                writer.append(item);
            }
            writer.close();
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int benchmarkSerializeOneByteArrayWithSchema(Collection<T> items) {
        return serializeOneByteArrayWithSchema(dataGenerator.getSchema(), items).length;
    }

    public static <T> byte[] serializeOneByteArrayWithoutSchema(Schema schema, Collection<T> items) {
        DatumWriter datumWriter = new SpecificDatumWriter(schema);
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
            for (T item : items) {
                datumWriter.write(item, encoder);
            }
            encoder.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int benchmarkSerializeOneByteArrayWithoutSchema(Collection<T> items) {
        return serializeOneByteArrayWithoutSchema(dataGenerator.getSchema(), items).length;
    }

    private int serializeManyByteArraysWithSchemaNewSerializer(Collection<T> items) {
        try {
            int len = 0;
            for (T item : items) {
                DatumWriter datumWriter = new SpecificDatumWriter(dataGenerator.getSchema());
                DataFileWriter<T> writer = new DataFileWriter<>(datumWriter);
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                writer.create(dataGenerator.getSchema(), bos);
                writer.append(item);
                writer.flush();
                byte[] bytes = bos.toByteArray();
                len += bytes.length;
            }
            return len;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int serializeManyByteArraysWithoutSchemaNewSerializer(Collection<T> items) {
        try {
            int len = 0;
            for (T item : items) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DatumWriter datumWriter = new SpecificDatumWriter(dataGenerator.getSchema());
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
                datumWriter.write(item, encoder);
                encoder.flush();
                byte[] bytes = baos.toByteArray();
                len += bytes.length;
            }
            return len;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> List<byte[]> serializeManyByteArraysWithSchemaReuseSerializer(Schema schema, Collection<T> items) {
        try {
            DatumWriter datumWriter = new SpecificDatumWriter(schema);
            DataFileWriter<T> writer = new DataFileWriter<>(datumWriter);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            List<byte[]> blobs = new ArrayList<>();
            for (T item : items) {
                bos.reset();
                writer.create(schema, bos);
                writer.append(item);
                writer.close();
                byte[] blob = bos.toByteArray();
                blobs.add(blob);
            }
            return blobs;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int benchmarkSerializeManyByteArraysWithSchemaReuseSerializer(Collection<T> items) {
        return sumArrayLengths(serializeManyByteArraysWithSchemaReuseSerializer(dataGenerator.getSchema(), items));
    }

    public static <T> List<byte[]> serializeManyByteArraysWithoutSchemaReuseSerializer(Schema schema, Collection<T> items) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DatumWriter datumWriter = new SpecificDatumWriter(schema);
            BinaryEncoder encoder = null;

            List<byte[]> blobs = new ArrayList<>();
            for (T item : items) {
                baos.reset();
                encoder = EncoderFactory.get().binaryEncoder(baos, encoder);
                datumWriter.write(item, encoder);
                encoder.flush();
                byte[] blob = baos.toByteArray();
                blobs.add(blob);
            }
            return blobs;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int benchmarkSerializeManyByteArraysWithoutSchemaReuseSerializer(Collection<T> items) {
        return sumArrayLengths(serializeManyByteArraysWithoutSchemaReuseSerializer(dataGenerator.getSchema(), items));
    }

}
