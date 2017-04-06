package net.batkin.s11n.data;

import net.batkin.s11n.data.model.Order;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataGenerator {

    public static final int NUM_ORDERS = 1_000_000;
    public static final int NUM_RUNS = 10;

    public static List<Order> generateOrders(int numberOfOrders) {
        List<Order> orders = new ArrayList<>();
        for (int orderNumber = 0; orderNumber < numberOfOrders; orderNumber++) {
            orders.add(new Order("ticker" + orderNumber, randomNumber(-1_000_000_000, 1_000_000_000), randomNumber(1000, 100_000), "strategy" + orderNumber));
        }
        return orders;
    }

    private static Random random = new Random();

    private static int randomNumber(int min, int max) {
        int size = max - min;
        return ((int)random.nextFloat() * size) + min;
    }

}
