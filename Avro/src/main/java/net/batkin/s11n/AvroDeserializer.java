package net.batkin.s11n;

import net.batkin.s11n.avro.generated.AvroOrder;
import net.batkin.s11n.data.model.Order;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static net.batkin.s11n.AvroSerializer.*;
import static net.batkin.s11n.data.DataGenerator.NUM_ORDERS;
import static net.batkin.s11n.data.DataGenerator.NUM_RUNS;
import static net.batkin.s11n.data.Stopwatch.timeSeries;

public class AvroDeserializer {

    public static void main(String[] args) throws Exception {
        List<AvroOrder> avroOrders = generateAvroOrders(NUM_ORDERS);

        List<byte[]> oneBlobWithSchema = Collections.singletonList(serializeOrdersWithSchema(avroOrders));
        List<byte[]> oneBlobWithoutSchema = Collections.singletonList(serializeOrdersWithoutSchema(avroOrders));
        List<byte[]> manyBlobsWithSchema = avroOrders.stream().map(item -> serializeOrdersWithSchema(Collections.singletonList(item))).collect(Collectors.toList());
        List<byte[]> manyBlobsWithoutSchema = avroOrders.stream().map(item -> serializeOrdersWithoutSchema(Collections.singletonList(item))).collect(Collectors.toList());

        timeSeries("Deserialize with schema (one byte array)", NUM_RUNS, () -> deserializeWithEmbeddedSchema(oneBlobWithSchema));
        timeSeries("Deserialize without schema (one byte array)", NUM_RUNS, () -> deserializeWithoutEmbeddedSchema(oneBlobWithoutSchema));
        timeSeries("Deserialize with schema (many byte arrays) no-reuse", NUM_RUNS, () -> deserializeWithEmbeddedSchema(manyBlobsWithSchema));
        timeSeries("Deserialize without schema (many byte arrays) no-reuse", NUM_RUNS, () -> deserializeWithoutEmbeddedSchema(manyBlobsWithoutSchema));
        timeSeries("Deserialize with schema (many byte arrays) with-reuse", NUM_RUNS, () -> deserializeWithEmbeddedSchemaWithReuse(manyBlobsWithSchema));
        timeSeries("Deserialize without schema (many byte arrays) with-reuse", NUM_RUNS, () -> deserializeWithoutEmbeddedSchemaWithReuse(manyBlobsWithoutSchema));
    }

    private static void deserializeWithEmbeddedSchema(List<byte[]> blobs) {
        for (byte[] b : blobs) {
            deserializeWithEmbeddedSchema(b);
        }
    }

    private static void deserializeWithoutEmbeddedSchema(List<byte[]> blobs) {
        for (byte[] b : blobs) {
            deserializeWithoutEmbeddedSchema(b);
        }
    }

    public static List<Order> deserializeWithEmbeddedSchema(byte[] bytes) {
        try {
            DatumReader<AvroOrder> datumReader = new SpecificDatumReader<>(AvroOrder.class);
            DataFileReader<AvroOrder> dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(bytes), datumReader);
            AvroOrder avroOrder = null;
            List<Order> orders = new ArrayList<>();
            while (dataFileReader.hasNext()) {
                avroOrder = dataFileReader.next(avroOrder);
                orders.add(toOrder(avroOrder));
            }
            return orders;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Order> deserializeWithEmbeddedSchemaWithReuse(List<byte[]> blobs) {
        try {
            DatumReader<AvroOrder> datumReader = new SpecificDatumReader<>(AvroOrder.class);
            AvroOrder avroOrder = null;
            List<Order> orders = new ArrayList<>();
            for (byte[] blob : blobs) {
                DataFileReader<AvroOrder> dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(blob), datumReader);
                while (dataFileReader.hasNext()) {
                    avroOrder = dataFileReader.next(avroOrder);
                    orders.add(toOrder(avroOrder));
                }
            }
            return orders;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Order> deserializeWithoutEmbeddedSchema(byte[] bytes) {
        try {
            DatumReader<AvroOrder> datumReader = new SpecificDatumReader<>(AvroOrder.class);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bytes), null);
            AvroOrder avroOrder = null;
            List<Order> orders = new ArrayList<>();
            while (!decoder.isEnd()) {
                avroOrder = datumReader.read(avroOrder, decoder);
                orders.add(toOrder(avroOrder));
            }
            return orders;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Order> deserializeWithoutEmbeddedSchemaWithReuse(List<byte[]> blobs) {
        try {
            DatumReader<AvroOrder> datumReader = new SpecificDatumReader<>(AvroOrder.class);
            BinaryDecoder decoder = null;
            AvroOrder avroOrder = null;
            List<Order> orders = new ArrayList<>();
            for (byte[] blob : blobs) {
                decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(blob), decoder);;
                avroOrder = datumReader.read(avroOrder, decoder);
                orders.add(toOrder(avroOrder));
            }
            return orders;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Order toOrder(AvroOrder avroOrder) {
        return new Order(avroOrder.getTicker(), avroOrder.getQuantity(), avroOrder.getAccountNumber(), avroOrder.getStrategy());
    }

}
