package net.batkin.s11n;

import net.batkin.s11n.avro.generated.AvroOrder;
import net.batkin.s11n.data.DataGenerator;
import net.batkin.s11n.data.model.Order;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static net.batkin.s11n.data.DataGenerator.NUM_ORDERS;
import static net.batkin.s11n.data.DataGenerator.NUM_RUNS;
import static net.batkin.s11n.data.Stopwatch.timeSeries;

public class AvroSerializer {

    public static void main(String[] args) throws Exception {
        List<AvroOrder> orders = generateAvroOrders(NUM_ORDERS);

        timeSeries("Serialize with schema (one byte array)", NUM_RUNS, () -> serializeOrdersWithSchema(orders));
        timeSeries("Serialize without schema (one byte array)", NUM_RUNS, () -> serializeOrdersWithoutSchema(orders));
        timeSeries("Serialize with schema (many byte arrays) no-reuse", NUM_RUNS, () -> orders.forEach((item) -> serializeOrdersWithSchema(Collections.singletonList(item))));
        timeSeries("Serialize without schema (many byte arrays) no-reuse", NUM_RUNS, () -> orders.forEach((item) -> serializeOrdersWithoutSchema(Collections.singletonList(item))));
        timeSeries("Serialize with schema (many byte arrays) with-reuse", NUM_RUNS, () -> serializeOrdersWithSchemaWithReuse(orders));
        timeSeries("Serialize without schema (many byte arrays) with-reuse", NUM_RUNS, () -> serializeOrdersWithoutSchemaWithReuse(orders));


//        try (FileOutputStream fos = new FileOutputStream("/home/abatkin/serial/avro/thin-1.bin")) {
//            List<AvroOrder> orders = generateAvroOrders(1);
//            fos.write(serializeOrdersWithoutSchema(orders));
//        }
//
//        try (FileOutputStream fos = new FileOutputStream("/home/abatkin/serial/avro/thin-1000.bin")) {
//            List<AvroOrder> orders = generateAvroOrders(1000);
//            fos.write(serializeOrdersWithoutSchema(orders));
//        }
//
//        try (FileOutputStream fos = new FileOutputStream("/home/abatkin/serial/avro/thin-1000000.bin")) {
//            List<AvroOrder> orders = generateAvroOrders(1000000);
//            fos.write(serializeOrdersWithoutSchema(orders));
//        }

    }

    public static List<AvroOrder> generateAvroOrders(int numOrders) {
        List<Order> orders = DataGenerator.generateOrders(numOrders);
        return orders
                .stream()
                .map(AvroSerializer::orderFromModel)
                .collect(Collectors.toList());
    }

    public static byte[] serializeOrdersWithSchema(List<AvroOrder> orders) {
        DatumWriter datumWriter = new SpecificDatumWriter(AvroOrder.class);
        DataFileWriter<AvroOrder> writer = new DataFileWriter<>(datumWriter);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            writer.create(AvroOrder.getClassSchema(), bos);
            for (AvroOrder order : orders) {
                writer.append(order);
            }
            writer.close();
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static List<byte[]> serializeOrdersWithSchemaWithReuse(List<AvroOrder> orders) {
        try {
            DatumWriter datumWriter = new SpecificDatumWriter(AvroOrder.class);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            List<byte[]> byteArrays = new ArrayList<>();
            for (AvroOrder order : orders) {
                bos.reset();
                DataFileWriter<AvroOrder> writer = new DataFileWriter<>(datumWriter);
                writer.create(AvroOrder.getClassSchema(), bos);
                writer.append(order);
                writer.close();
                byte[] bytes = bos.toByteArray();
                byteArrays.add(bytes);
            }
            return byteArrays;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] serializeOrdersWithoutSchema(List<AvroOrder> orders) {
        DatumWriter datumWriter = new SpecificDatumWriter(AvroOrder.class);
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
            for (AvroOrder order : orders) {
                datumWriter.write(order, encoder);
            }
            encoder.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static List<byte[]> serializeOrdersWithoutSchemaWithReuse(List<AvroOrder> orders) {
        try {
            DatumWriter datumWriter = new SpecificDatumWriter(AvroOrder.class);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            BinaryEncoder encoder = null;
            List<byte[]> byteArrays = new ArrayList<>();
            for (AvroOrder order : orders) {
                bos.reset();
                encoder = EncoderFactory.get().binaryEncoder(bos, encoder);
                datumWriter.write(order, encoder);
                encoder.flush();
                byte[] bytes = bos.toByteArray();
                byteArrays.add(bytes);
            }
            return byteArrays;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
