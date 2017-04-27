package net.batkin.s11n;

import net.batkin.s11n.serializer.Serializer;

public interface SerializerProvider<T> {
    Serializer<T> getSerializer();
}
