package net.batkin.s11n.data;

import java.util.Collection;

public class Stopwatch<T> {

    private static final long NANOS_TO_SECONDS = 1_000_000_000;

    private Collection<T> items;
    private BenchmarkStatistics statistics;
    private String language;
    private int runs;

    public Stopwatch(String language, int runs, Collection<T> items) {
        this.language = language;
        this.runs = runs;
        this.items = items;
    }

    private <T> void time(BenchmarkStatistics stats, Collection<T> items, TimeableOperation<T> func) {
        long nanoStart = System.nanoTime();
        int result = func.run(items);
        long nanoEnd = System.nanoTime();
        long nanos = nanoEnd - nanoStart;
        double seconds = (double)nanos / (double)NANOS_TO_SECONDS;
        stats.finishIteration(seconds, result);
    }

    public BenchmarkStatistics timeSeries(String options, String operation, TimeableOperation<T> func) {
        int itemCount = items.size();
        BenchmarkStatistics stats = new DescriptiveBenchmarkStatistics(language, options, operation, itemCount);
        for (int run = 0; run < runs; run++) {
            time(stats, items, func);
        }
        return stats;
    }

}
