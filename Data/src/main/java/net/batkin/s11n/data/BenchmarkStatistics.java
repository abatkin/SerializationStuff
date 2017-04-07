package net.batkin.s11n.data;

public interface BenchmarkStatistics {

	void finishIteration(double time, int result);

	double runtimeMin();
	double runtimeMax();
	double runtimeMean();
	double runtimeStdDev();
	double runtimeTotal();

	int iterations();
	int itemCount();
	long resultCount();

	String[] headings();
	Object[] toValues();
}
