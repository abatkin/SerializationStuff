package net.batkin.s11n;

import net.batkin.s11n.avro.generated.AvroOrder;
import net.batkin.s11n.data.BenchmarkRunner;
import net.batkin.s11n.serializer.SerializerWithSchema;
import net.batkin.s11n.serializer.SerializerWithoutSchema;

import java.util.List;

import static net.batkin.s11n.data.DataGenerator.NUM_ORDERS;
import static net.batkin.s11n.data.DataGenerator.NUM_RUNS;

public class AvroSerializer<T> {

    public static final String OPERATION_SERIALIZE = "Serialize";

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

        SerializationSet.runBenchmarks(runner, items, () -> new SerializerWithSchema<>(dataGenerator.getSchema()));
        SerializationSet.runBenchmarks(runner, items, () -> new SerializerWithoutSchema<>(dataGenerator.getSchema()));
    }

}
