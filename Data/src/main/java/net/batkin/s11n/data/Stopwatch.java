package net.batkin.s11n.data;

public class Stopwatch {

    private static final long NANOS_TO_SECONDS = 1_000_000_000;

    private BenchmarkStatistics statistics;
    private String language;
    private int runs;

    public Stopwatch(String language, int runs) {
        this.language = language;
        this.runs = runs;
    }

    private void time(BenchmarkStatistics stats, TimeableOperation func) {
        long nanoStart = System.nanoTime();
        int result = func.run();
        long nanoEnd = System.nanoTime();
        long nanos = nanoEnd - nanoStart;
        double seconds = (double)nanos / (double)NANOS_TO_SECONDS;
        stats.finishIteration(seconds, result);
    }

    public BenchmarkStatistics timeSeries(String options, String operation, int itemCount, TimeableOperation func) {
        BenchmarkStatistics stats = new DescriptiveBenchmarkStatistics(language, options, operation, itemCount);
        for (int run = 0; run < runs; run++) {
            time(stats, func);
        }
        return stats;
    }

}
