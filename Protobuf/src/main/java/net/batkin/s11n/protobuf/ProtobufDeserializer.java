package net.batkin.s11n.protobuf;

import net.batkin.s11n.data.model.Order;
import net.batkin.s11n.protobuf.generated.ProtobufOrder;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static net.batkin.s11n.data.DataGenerator.NUM_ORDERS;
import static net.batkin.s11n.data.DataGenerator.NUM_RUNS;
import static net.batkin.s11n.data.Stopwatch.timeSeries;
import static net.batkin.s11n.protobuf.ProtobufSerializer.generateProtobufOrders;
import static net.batkin.s11n.protobuf.ProtobufSerializer.serializeOrders;

public class ProtobufDeserializer {

    public static void main(String[] args) throws Exception {
        List<ProtobufOrder.Order> orders = generateProtobufOrders(NUM_ORDERS);
        List<byte[]> oneBlob = serializeOrders(orders);
        List<byte[]> manyBlobs = new ArrayList<>();
        for (ProtobufOrder.Order order : orders) {
            manyBlobs.add(serializeOrders(Collections.singletonList(order)).get(0));
        }

        System.out.println("Generated!");

//        timeSeries("Deserialize (one byte array)", NUM_RUNS, () -> deserialize(oneBlob));
        timeSeries("Deserialize (many byte arrays)", NUM_RUNS, () -> deserialize(manyBlobs));
    }

    private static List<Order> deserialize(List<byte[]> blobs) {
        try {
            List<Order> orders = new ArrayList<>();
            for (byte[] blob : blobs) {
                try (ByteArrayInputStream bis = new ByteArrayInputStream(blob)) {
                    while (bis.available() > 0) {
                        ProtobufOrder.Order protobufOrder = ProtobufOrder.Order.parseFrom(bis);
                        Order order = toOrder(protobufOrder);
                        orders.add(order);
                    }
                }
            }
            return orders;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Order toOrder(ProtobufOrder.Order protobufOrder) {
        return new Order(protobufOrder.getTicker(), protobufOrder.getQuantity(), protobufOrder.getAccountNumber(), protobufOrder.getStrategy());
    }

}
