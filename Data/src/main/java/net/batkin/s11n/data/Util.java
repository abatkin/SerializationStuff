package net.batkin.s11n.data;

import java.util.List;

public class Util {
    public static int sumArrayLengths(List<byte[]> blobs) {
        return blobs.stream().map(b -> b.length).reduce(0, (a, b) -> a + b);
    }


}
