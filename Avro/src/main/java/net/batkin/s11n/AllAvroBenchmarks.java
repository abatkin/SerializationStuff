package net.batkin.s11n;

import net.batkin.s11n.data.BenchmarkRunner;

import java.io.IOException;

public class AllAvroBenchmarks {
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: AllAvroBenchmarks numRuns numOrders csvPath");
            System.exit(1);
        }

        int numRuns = Integer.parseInt(args[0]);
        int numOrders = Integer.parseInt(args[1]);
        String filename = args[2];

        BenchmarkRunner runner = new BenchmarkRunner(numRuns);
        AvroSerializer.runBenchmarks(numOrders, runner);
        AvroDeserializer.runBenchmarks(numOrders, runner);
        runner.dumpCsv(filename);
    }
}
