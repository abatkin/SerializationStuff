package net.batkin.s11n.protobuf;

import net.batkin.s11n.data.DataGenerator;
import net.batkin.s11n.data.model.Order;
import net.batkin.s11n.protobuf.generated.ProtobufOrder;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static net.batkin.s11n.data.DataGenerator.NUM_ORDERS;
import static net.batkin.s11n.data.DataGenerator.NUM_RUNS;

public class ProtobufSerializer {

    public static void main(String[] args) throws Exception {
//        generateFile("/home/abatkin/serial/protobuf", 1);
//        generateFile("/home/abatkin/serial/protobuf", 1000);
//        generateFile("/home/abatkin/serial/protobuf", 1_000_000);

        List<ProtobufOrder.Order> orders = generateProtobufOrders(NUM_ORDERS);

//        timeSeries("Serialize (one byte array)", NUM_RUNS, () -> serializeOrders(orders));
//        timeSeries("Serialize (many byte arrays)", NUM_RUNS, () -> orders.stream().map((item) -> serializeOrders(Collections.singletonList(item))).collect(Collectors.toList()));
    }

    public static List<byte[]> serializeOrders(List<ProtobufOrder.Order> orders) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            for (ProtobufOrder.Order order : orders) {
                order.writeTo(baos);
            }
            return Collections.singletonList(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void generateFile(String path, int num) throws IOException {
        File file = new File(path, "protobuf-order-" + num + ".bin");
        try (FileOutputStream fos = new FileOutputStream(file)) {
            List<ProtobufOrder.Order> orders = generateProtobufOrders(num);
            for (ProtobufOrder.Order order : orders) {
                order.writeTo(fos);
            }
        }
    }

    public static List<ProtobufOrder.Order> generateProtobufOrders(int numOrders) {
        List<Order> orders = DataGenerator.generateOrders(numOrders);
        return orders
                .stream()
                .map(ProtobufSerializer::orderFromModel)
                .collect(Collectors.toList());

    }

    private static ProtobufOrder.Order orderFromModel(Order order) {
        return ProtobufOrder.Order.newBuilder()
                .setTicker(order.getTicker())
                .setQuantity(order.getQuantity())
                .setAccountNumber(order.getAccountNumber())
                .setStrategy(order.getStrategy())
                .build();
    }
}
