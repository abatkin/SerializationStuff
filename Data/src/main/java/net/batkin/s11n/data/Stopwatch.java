package net.batkin.s11n.data;

import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;

public class Stopwatch {

    public static long time(String name, Supplier<? extends Collection<?>> runnable) {
        long nanoStart = System.nanoTime();
//        System.out.println(name + ": " + runnable.get().size());
        long nanoEnd = System.nanoTime();

        long nanos = nanoEnd - nanoStart;
        return nanos;
    }

    public static void timeSeries(String name, int runs, Supplier<? extends Collection<?>> runnable) {
        long min = 0;
        long max = 0;
        long total = 0;
        for (int run = 0; run < runs; run++) {
            long time = time(name, runnable);
            total += time;
            if (min == 0 || time < min) {
                min = time;
            }
            if (max == 0 || time > max) {
                max = time;
            }
        }
        long nanos = total / runs;

        dumpTimes("min", name, min);
        dumpTimes("max", name, max);
        dumpTimes("total", name, total);
        dumpTimes("avg", name, nanos);
    }

    private static void dumpTimes(String metric, String name, long nanos) {
        long millis = nanos / 1_000_000;
        double secs = nanos / 1_000_000_000.0d;

        System.out.println("[" + metric + "] [" + name + "] " + nanos + "ns, " + millis + "ms, " + secs + "secs");
    }

}
