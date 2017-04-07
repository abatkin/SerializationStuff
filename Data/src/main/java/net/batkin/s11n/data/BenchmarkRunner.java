package net.batkin.s11n.data;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static net.batkin.s11n.data.DataGenerator.LANGUAGE_JAVA;

public class BenchmarkRunner {

	private List<BenchmarkStatistics> stats = new ArrayList<>();
	private int numRuns;

	public BenchmarkRunner(int numRuns) {
		this.numRuns = numRuns;
	}

	public <T> void runBenchmarks(Collection<T> items, BenchmarkRun... runs) {
		Stopwatch<T> stopwatch = new Stopwatch<T>(LANGUAGE_JAVA, numRuns, items);
		for (BenchmarkRun run : runs) {
			stats.add(stopwatch.timeSeries(run.getOptions(), run.getOperation(), run.getFunction()));
		}
	}

	public void dumpCsv() throws IOException {
		CSVFormat csvFormat = CSVFormat.EXCEL;
		CSVPrinter printer = new CSVPrinter(System.out, csvFormat);
		printer.printRecord(stats.get(0).headings());
		for (BenchmarkStatistics stat : stats) {
			printer.printRecord(stat.toValues());
		}
	}
}
