package net.batkin.s11n.data;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static net.batkin.s11n.data.DataGenerator.LANGUAGE_JAVA;

public class BenchmarkRunner {

	private List<BenchmarkStatistics> stats = new ArrayList<>();
	private int numRuns;

	public BenchmarkRunner(int numRuns) {
		this.numRuns = numRuns;
	}

	public void runBenchmarks(BenchmarkRun... runs) {
		Stopwatch stopwatch = new Stopwatch(LANGUAGE_JAVA, numRuns);
		for (BenchmarkRun run : runs) {
			System.out.println("Running " + run.getOptions() + " (" + run.getOperation() + ")");
			stats.add(stopwatch.timeSeries(run.getOptions(), run.getOperation(), run.getItemCount(), run.getFunction()));
			System.out.println("Finished " + run.getOptions() + " (" + run.getOperation() + ")");
		}
	}

	public void dumpCsv(String filename) throws IOException {
		try (FileWriter writer = new FileWriter(filename)) {
			dumpCsv(writer);
		}
	}

	public void dumpCsv(Appendable out) throws IOException {
		CSVFormat csvFormat = CSVFormat.EXCEL;
		CSVPrinter printer = new CSVPrinter(out, csvFormat);
		printer.printRecord(stats.get(0).headings());
		for (BenchmarkStatistics stat : stats) {
			printer.printRecord(stat.toValues());
		}
	}
}
