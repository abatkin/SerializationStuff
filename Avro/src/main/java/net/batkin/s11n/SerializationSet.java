package net.batkin.s11n;

import net.batkin.s11n.data.BenchmarkRunner;
import net.batkin.s11n.serializer.Serializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static net.batkin.s11n.AvroSerializer.OPERATION_SERIALIZE;
import static net.batkin.s11n.data.BenchmarkRun.r;

public class SerializationSet<T> {

    private SerializerProvider<T> serializerProvider;

    public SerializationSet(SerializerProvider<T> serializerProvider) {
        this.serializerProvider = serializerProvider;
    }

    public byte[] serializeOneByteArray(Collection<T> items) {
        try {
            Serializer<T> serializer = serializerProvider.getSerializer();
            for (T item : items) {
                serializer.serialize(item);
            }
            return serializer.getBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<byte[]> serializeManyByteArraysNoReuse(Collection<T> items) {
        try {
            List<byte[]> blobs = new ArrayList<>();
            for (T item : items) {
                Serializer<T> serializer = serializerProvider.getSerializer();
                serializer.serialize(item);
                byte[] bytes = serializer.getBytes();
                blobs.add(bytes);
            }
            return blobs;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<byte[]> serializeManyByteArraysWithReuse(Collection<T> items) {
        try {
            Serializer<T> serializer = serializerProvider.getSerializer();
            List<byte[]> blobs = new ArrayList<>();
            for (T item : items) {
                serializer.serialize(item);
                byte[] bytes = serializer.getBytes();
                blobs.add(bytes);
            }
            return blobs;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> void runBenchmarks(BenchmarkRunner runner, Collection<T> items, SerializerProvider<T> serializerProvider) {
        SerializationSet<T> serializationSet = new SerializationSet<>(serializerProvider);
        String serializerType = serializerProvider.getSerializer().getClass().getSimpleName();
        runner.runBenchmarks(
                r(serializerType + ", One Byte Array", OPERATION_SERIALIZE, items.size(), () -> serializationSet.serializeOneByteArray(items).length),
                r(serializerType + ", Many Byte Arrays, New Serializer", OPERATION_SERIALIZE, items.size(), () -> addBytes(serializationSet.serializeManyByteArraysNoReuse(items))),
                r(serializerType + ", Many Byte Arrays, Reuse Serializer", OPERATION_SERIALIZE, items.size(), () -> addBytes(serializationSet.serializeManyByteArraysWithReuse(items)))
        );
    }

    private static int addBytes(List<byte[]> blobs) {
        int len = 0;
        for (byte[] blob : blobs) {
            len += blob.length;
        }
        return len;
    }
}
