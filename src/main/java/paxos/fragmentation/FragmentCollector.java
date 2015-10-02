package paxos.fragmentation;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;

class FragmentCollector {
    private final byte[][] parts;
    private int partsReceived = 0;

    public FragmentCollector(int parts) {
        this.parts = new byte[parts][];
    }

    public void addPart(int partNo, byte[] bytes) {
        parts[partNo] = bytes;
        partsReceived++;
    }

    public boolean isComplete() {
        if (partsReceived < parts.length) return false; // this is just an optimisation

        // we still need this if we received the same message twice
        for (byte[] part : parts) {
            if (part == null) return false;
        }
        return true;
    }

    public Serializable extractMessage() {
        try {
            int totalBytes = 0;
            for (byte[] part : parts) totalBytes += part.length;

            byte[] concatenated = new byte[totalBytes];
            int cursor = 0;
            for (byte[] part : parts) {
                System.arraycopy(part, 0, concatenated, cursor, part.length);
                cursor += part.length;
            }

            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(concatenated));
            return (Serializable) ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException("Could not deserialize concatenated message", e);
        }
    }
}
