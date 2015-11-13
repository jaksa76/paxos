package paxos;

import java.util.HashSet;
import java.util.Set;

/**
 * Keeps track of messages that have been received.
 */
public class MissingMessagesTracker {
    private long tail = 0; // all messages before the tail have been received
    private Set<Long> received = new HashSet<Long>(); // received messages after the tail

    /**
     * Mark the specified message as received.
     *
     * @param seqNo
     */
    public void received(long seqNo) {
        if (tail == seqNo) {
            tail++;
            advanceTail();
        } else {
            received.add(seqNo);
        }
    }

    /**
     * advance the tail to the next missing message
     */
    private void advanceTail() {
        while (!received.isEmpty()) {
            if (!received.contains(tail)) return;
            received.remove(tail);
            tail++;
        }
    }

    /**
     * Returns the messages that are missing up to the specified one.
     *
     * @param seqNo
     * @return
     */
    public Set<Long> getMissing(long seqNo) {
        Set<Long> missingSuccess = new HashSet<Long>();
        for (long i = tail; i < seqNo; i++) {
            if (!received.contains(i)) missingSuccess.add(i);
        }
        return missingSuccess;
    }
}
