package net.batkin.s11n.data;

import java.util.Collection;

public interface TimeableOperation<T> {
	int run(T input);
}
