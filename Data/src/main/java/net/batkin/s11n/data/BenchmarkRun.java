package net.batkin.s11n.data;

public class BenchmarkRun {

	private String options;
	private String operation;
	private int itemCount;
	private TimeableOperation function;

	public BenchmarkRun(String options, String operation, int itemCount, TimeableOperation function) {
		this.options = options;
		this.operation = operation;
		this.itemCount = itemCount;
		this.function = function;
	}

	public String getOptions() {
		return options;
	}

	public String getOperation() {
		return operation;
	}

	public TimeableOperation getFunction() {
		return function;
	}

	public int getItemCount() {
		return itemCount;
	}

	public static <T> BenchmarkRun r (String options, String operation, int itemCount, TimeableOperation function) {
		return new BenchmarkRun(options, operation, itemCount, function);
	}
}
