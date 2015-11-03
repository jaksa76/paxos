package paxos.fragmentation;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;

/**
 * A utility class for collecting fragments of a single message.
 */
class FragmentCollector {
    private final byte[][] parts;
    private int partsReceived = 0;

    /**
     * @param parts the number of parts that need to be collected
     */
    public FragmentCollector(int parts) {
        this.parts = new byte[parts][];
    }

    public void addPart(int partNo, byte[] bytes) {
        parts[partNo] = bytes;
        partsReceived++;
    }

    public boolean isComplete() {
        if (partsReceived < parts.length) return false; // this is just an optimisation

        // we still need this in case we received the same message twice
        for (byte[] part : parts) {
            if (part == null) return false;
        }
        return true;
    }

    public byte[] extractMessage() {
        int totalBytes = 0;
        for (byte[] part : parts) totalBytes += part.length;

        byte[] concatenated = new byte[totalBytes];
        int cursor = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, concatenated, cursor, part.length);
            cursor += part.length;
        }

        return concatenated;
    }
}
