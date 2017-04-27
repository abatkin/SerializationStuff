package net.batkin.s11n.serializer;

import net.batkin.s11n.avro.serializer.AvroSerializerBenchmarks;
import net.batkin.s11n.data.BenchmarkRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static net.batkin.s11n.data.BenchmarkRun.r;

public class SerializationSet<T> {

    private Supplier<Serializer<T>> serializerProvider;

    public SerializationSet(Supplier<Serializer<T>> serializerProvider) {
        this.serializerProvider = serializerProvider;
    }

    public byte[] serializeOneByteArray(Collection<T> items) {
        try {
            Serializer<T> serializer = serializerProvider.get();
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
                Serializer<T> serializer = serializerProvider.get();
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
            Serializer<T> serializer = serializerProvider.get();
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

    public static <T> void runBenchmarks(BenchmarkRunner runner, Collection<T> items, Supplier<Serializer<T>> serializerProvider) {
        SerializationSet<T> serializationSet = new SerializationSet<>(serializerProvider);
        String serializerType = serializerProvider.get().getClass().getSimpleName();
        runner.runBenchmarks(
                r(serializerType + ", One Byte Array", AvroSerializerBenchmarks.OPERATION_SERIALIZE, items.size(), () -> serializationSet.serializeOneByteArray(items).length),
                r(serializerType + ", Many Byte Arrays, New Serializer", AvroSerializerBenchmarks.OPERATION_SERIALIZE, items.size(), () -> addBytes(serializationSet.serializeManyByteArraysNoReuse(items))),
                r(serializerType + ", Many Byte Arrays, Reuse Serializer", AvroSerializerBenchmarks.OPERATION_SERIALIZE, items.size(), () -> addBytes(serializationSet.serializeManyByteArraysWithReuse(items)))
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
