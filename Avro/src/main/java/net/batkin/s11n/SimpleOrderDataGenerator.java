package net.batkin.s11n;

import net.batkin.s11n.avro.generated.AvroOrder;
import net.batkin.s11n.data.model.Order;
import org.apache.avro.Schema;

import static net.batkin.s11n.data.DataGenerator.generateOrder;

public class SimpleOrderDataGenerator implements AvroDataGenerator<AvroOrder> {
    @Override
    public AvroOrder generateDatum(int id) {
        Order order = generateOrder(id);
        return orderFromModel(order);
    }

    @Override
    public Schema getSchema() {
        return AvroOrder.getClassSchema();
    }

    private static AvroOrder orderFromModel(Order order) {
        return AvroOrder.newBuilder()
                .setTicker(order.getTicker())
                .setQuantity(order.getQuantity())
                .setAccountNumber(order.getAccountNumber())
                .setStrategy(order.getStrategy())
                .build();
    }
}
