package net.batkin.s11n.data;

public class BenchmarkRun<T> {

	private String options;
	private String operation;
	private int itemCount;
	private T data;
	private TimeableOperation<T> function;

	public BenchmarkRun(String options, String operation, int itemCount, T data, TimeableOperation<T> function) {
		this.options = options;
		this.operation = operation;
		this.itemCount = itemCount;
		this.data = data;
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

	public int getItemCount() {
		return itemCount;
	}

	public T getData() {
		return data;
	}

	public static <T> BenchmarkRun r (String options, String operation, int itemCount, T data, TimeableOperation<T> function) {
		return new BenchmarkRun(options, operation, itemCount, data, function);
	}
}
