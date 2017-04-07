package net.batkin.s11n;

import net.batkin.s11n.avro.generated.AvroOrder;
import net.batkin.s11n.data.BenchmarkRunner;
import net.batkin.s11n.data.DataGenerator;
import net.batkin.s11n.data.model.Order;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static net.batkin.s11n.data.BenchmarkRun.r;
import static net.batkin.s11n.data.DataGenerator.NUM_ORDERS;
import static net.batkin.s11n.data.DataGenerator.NUM_RUNS;

public class AvroSerializer {

    private static final String OPERATION_SERIALIZE = "Serialize";

    public static void main(String[] args) throws Exception {
        BenchmarkRunner runner = new BenchmarkRunner(NUM_RUNS);
        runBenchmarks(runner);
        runner.dumpCsv();
    }

    public static void runBenchmarks(BenchmarkRunner runner) {
        List<AvroOrder> orders = generateAvroOrders(NUM_ORDERS);
        runner.runBenchmarks(orders,
                r("With Schema, One Byte Array", OPERATION_SERIALIZE, AvroSerializer::serializeOneByteArrayWithSchema),
                r("Without Schema, One Byte Array", OPERATION_SERIALIZE, AvroSerializer::serializeOneByteArrayWithoutSchema),
                r("With Schema, Many Byte Arrays, New Serializer", OPERATION_SERIALIZE, AvroSerializer::serializeManyByteArraysWithSchemaNewSerializer),
                r("Without Schema, Many Byte Arrays, New Serializer", OPERATION_SERIALIZE, AvroSerializer::serializeManyByteArraysWithoutSchemaNewSerializer),
                r("With Schema, Many Byte Arrays, Reuse Serializer", OPERATION_SERIALIZE, AvroSerializer::serializeManyByteArraysWithSchemaReuseSerializer),
                r("Without Schema, Many Byte Arrays, Reuse Serializer", OPERATION_SERIALIZE, AvroSerializer::serializeManyByteArraysWithoutSchemaReuseSerializer)
        );
    }

    private static int serializeOneByteArrayWithSchema(Collection<AvroOrder> avroOrders) {
        DatumWriter datumWriter = new SpecificDatumWriter(AvroOrder.getClassSchema());
        DataFileWriter<AvroOrder> writer = new DataFileWriter<>(datumWriter);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            writer.create(AvroOrder.getClassSchema(), bos);
            for (AvroOrder order : avroOrders) {
                writer.append(order);
            }
            writer.close();
            return bos.toByteArray().length;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static int serializeOneByteArrayWithoutSchema(Collection<AvroOrder> avroOrders) {
        DatumWriter datumWriter = new SpecificDatumWriter(AvroOrder.getClassSchema());
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
            for (AvroOrder order : avroOrders) {
                datumWriter.write(order, encoder);
            }
            encoder.flush();
            return baos.toByteArray().length;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static int serializeManyByteArraysWithSchemaNewSerializer(Collection<AvroOrder> avroOrders) {
        try {
            int len = 0;
            for (AvroOrder order : avroOrders) {
                DatumWriter datumWriter = new SpecificDatumWriter(AvroOrder.getClassSchema());
                DataFileWriter<AvroOrder> writer = new DataFileWriter<>(datumWriter);
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                writer.create(AvroOrder.getClassSchema(), bos);
                writer.append(order);
                writer.flush();
                byte[] bytes = bos.toByteArray();
                len += bytes.length;
            }
            return len;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static int serializeManyByteArraysWithoutSchemaNewSerializer(Collection<AvroOrder> avroOrders) {
        try {
            int len = 0;
            for (AvroOrder order : avroOrders) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DatumWriter datumWriter = new SpecificDatumWriter(AvroOrder.getClassSchema());
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
                datumWriter.write(order, encoder);
                encoder.flush();
                byte[] bytes = baos.toByteArray();
                len += bytes.length;
            }
            return len;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static int serializeManyByteArraysWithSchemaReuseSerializer(Collection<AvroOrder> avroOrders) {
        try {
            DatumWriter datumWriter = new SpecificDatumWriter(AvroOrder.getClassSchema());
            DataFileWriter<AvroOrder> writer = new DataFileWriter<>(datumWriter);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            int len = 0;
            for (AvroOrder order : avroOrders) {
                bos.reset();
                writer.create(AvroOrder.getClassSchema(), bos);
                writer.append(order);
                writer.close();
                byte[] bytes = bos.toByteArray();
                len += bytes.length;
            }
            return len;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static int serializeManyByteArraysWithoutSchemaReuseSerializer(Collection<AvroOrder> avroOrders) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DatumWriter datumWriter = new SpecificDatumWriter(AvroOrder.getClassSchema());
            BinaryEncoder encoder = null;

            int len = 0;
            for (AvroOrder order : avroOrders) {
                baos.reset();
                encoder = EncoderFactory.get().binaryEncoder(baos, encoder);
                datumWriter.write(order, encoder);
                encoder.flush();
                byte[] bytes = baos.toByteArray();
                len += bytes.length;
            }
            return len;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static List<AvroOrder> generateAvroOrders(int numOrders) {
        List<Order> orders = DataGenerator.generateOrders(numOrders);
        return orders
                .stream()
                .map(AvroSerializer::orderFromModel)
                .collect(Collectors.toList());
    }

    public static AvroOrder orderFromModel(Order order) {
        return AvroOrder.newBuilder()
                .setTicker(order.getTicker())
                .setQuantity(order.getQuantity())
                .setAccountNumber(order.getAccountNumber())
                .setStrategy(order.getStrategy())
                .build();
    }

}
