package net.batkin.s11n.deserializer;

import net.batkin.s11n.data.BenchmarkRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static net.batkin.s11n.avro.deserializer.AvroDeserializerBenchmarks.OPERATION_DESERIALIZE;
import static net.batkin.s11n.data.BenchmarkRun.r;
import static net.batkin.s11n.data.Util.sumArrayLengths;

public class DeserializationSet<T> {

    private Supplier<Deserializer<T>> deserializerProvider;

    public DeserializationSet(Supplier<Deserializer<T>> deserializerProvider) {
        this.deserializerProvider = deserializerProvider;
    }

    public List<T> deserializeOneByteArray(byte[] bytes) {
        try {
            Deserializer<T> deserializer = deserializerProvider.get();
            return deserializer.deserialize(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<T> deserializeManyByteArraysNoReuse(List<byte[]> blobs) {
        try {
            List<T> items = new ArrayList<>();
            for (byte[] blob : blobs) {
                Deserializer<T> deserializer = deserializerProvider.get();
                items.addAll(deserializer.deserialize(blob));
            }
            return items;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<T> deserializeManyByteArraysWithReuse(List<byte[]> blobs) {
        try {
            List<T> items = new ArrayList<>();
            Deserializer<T> deserializer = deserializerProvider.get();
            for (byte[] blob : blobs) {
                items.addAll(deserializer.deserialize(blob));
            }
            return items;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> void runBenchmarks(BenchmarkRunner runner, byte[] oneBlob, List<byte[]> manyBlobs, Supplier<Deserializer<T>> deserializerProvider) {
        int manyBlobsLen = sumArrayLengths(manyBlobs);

        DeserializationSet<T> deserializationSet = new DeserializationSet<>(deserializerProvider);
        String deserializerType = deserializerProvider.get().getName();
        runner.runBenchmarks(
                r(deserializerType + ", One Byte Array", OPERATION_DESERIALIZE, oneBlob.length, () -> deserializationSet.deserializeOneByteArray(oneBlob).size()),
                r(deserializerType + ", Many Byte Arrays, New Deserializer", OPERATION_DESERIALIZE, manyBlobsLen, () -> deserializationSet.deserializeManyByteArraysNoReuse(manyBlobs).size()),
                r(deserializerType + ", Many Byte Arrays, Reuse Deserializer", OPERATION_DESERIALIZE, manyBlobsLen, () -> deserializationSet.deserializeManyByteArraysWithReuse(manyBlobs).size())
        );
    }
}
