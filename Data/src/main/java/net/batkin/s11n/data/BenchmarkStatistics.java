package net.batkin.s11n.data;

public interface BenchmarkStatistics {

	void finishIteration(double time, int result);

	double runtimeMin();
	double runtimeMax();
	double runtimeMean();
	double runtimeStdDev();
	double runtimeTotal();

	double resultMin();
	double resultMax();
	double resultMean();
	double resultStdDev();
	
	int iterations();
	int itemCount();

	String[] headings();
	Object[] toValues();
}
