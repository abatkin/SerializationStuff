package net.batkin.s11n.data;

public class Stopwatch<T> {

    private static final long NANOS_TO_SECONDS = 1_000_000_000;

    private BenchmarkStatistics statistics;
    private String language;
    private int runs;

    public Stopwatch(String language, int runs) {
        this.language = language;
        this.runs = runs;
    }

    private <T> void time(BenchmarkStatistics stats, T data, TimeableOperation<T> func) {
        long nanoStart = System.nanoTime();
        int result = func.run(data);
        long nanoEnd = System.nanoTime();
        long nanos = nanoEnd - nanoStart;
        double seconds = (double)nanos / (double)NANOS_TO_SECONDS;
        stats.finishIteration(seconds, result);
    }

    public BenchmarkStatistics timeSeries(String options, String operation, int itemCount, T data, TimeableOperation<T> func) {
        BenchmarkStatistics stats = new DescriptiveBenchmarkStatistics(language, options, operation, itemCount);
        for (int run = 0; run < runs; run++) {
            time(stats, data, func);
        }
        return stats;
    }

}
