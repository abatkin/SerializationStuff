package net.batkin.s11n.deserializer;

import java.io.IOException;
import java.util.List;

public interface Deserializer<T> {

    List<T> deserialize(byte[] bytes) throws IOException;

}
