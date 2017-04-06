package net.batkin.s11n.data;

public class BenchmarkRun<T> {

	private String options;
	private String operation;
	private TimeableOperation<T> function;

	public BenchmarkRun(String options, String operation, TimeableOperation<T> function) {
		this.options = options;
		this.operation = operation;
		this.function = function;
	}

	public String getOptions() {
		return options;
	}

	public String getOperation() {
		return operation;
	}

	public TimeableOperation<T> getFunction() {
		return function;
	}

	public static <T> BenchmarkRun r (String options, String operation, TimeableOperation<T> function) {
		return new BenchmarkRun(options, operation, function);
	}
}
