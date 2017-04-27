package net.batkin.s11n.data;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class DescriptiveBenchmarkStatistics implements BenchmarkStatistics {

	private static final String[] HEADINGS = new String[] {"Language", "Options", "Operation", "Iterations", "ItemCount", "ResultCount", "RuntimeMin", "RuntimeMax", "RuntimeMean", "RuntimeStdDev", "RuntimeTotal"};

	private String language;
	private String options;
	private String operation;
	private DescriptiveStatistics runtimeStatistics = new DescriptiveStatistics(DescriptiveStatistics.INFINITE_WINDOW);
	private long resultCount;
	private int iterations;
	private int items;

	public DescriptiveBenchmarkStatistics(String language, String options, String operation, int itemCount) {
		this.language = language;
		this.options = options;
		this.operation = operation;
		items = itemCount;
	}

	@Override
	public void finishIteration(double time, int result) {
		iterations++;
		runtimeStatistics.addValue(time);
		resultCount += result;
	}

	@Override
	public double runtimeMin() {
		return runtimeStatistics.getMin();
	}

	@Override
	public double runtimeMax() {
		return runtimeStatistics.getMax();
	}

	@Override
	public double runtimeMean() {
		return runtimeStatistics.getMean();
	}

	@Override
	public double runtimeStdDev() {
		return runtimeStatistics.getStandardDeviation();
	}

	@Override
	public double runtimeTotal() {
		return runtimeStatistics.getSum();
	}

	@Override
	public long resultCount() {
		return resultCount;
	}

	@Override
	public int iterations() {
		return iterations;
	}

	@Override
	public int itemCount() {
		return items;
	}

	@Override
	public String[] headings() {
		return HEADINGS;
	}

	@Override
	public Object[] toValues() {
		return new Object[] {language, options, operation, iterations(), itemCount(), resultCount() / iterations(), runtimeMin(), runtimeMax(), runtimeMean(), runtimeStdDev(), runtimeTotal()};
	}
}
