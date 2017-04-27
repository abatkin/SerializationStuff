package net.batkin.s11n.serializer;

import java.io.IOException;

public interface Serializer<T> {

    void serialize(T item) throws IOException;
    byte[] getBytes() throws IOException;

}
