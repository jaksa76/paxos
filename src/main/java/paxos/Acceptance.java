package paxos;

import java.io.Serializable;

/**
 * Entry about an accepted message.
 */
public class Acceptance implements Serializable {
    public long viewNumber;
    public Serializable message;
    public long msgId;

    public Acceptance(long viewNumber, Serializable message, long msgId) {
        this.viewNumber = viewNumber;
        this.message = message;
        this.msgId = msgId;
    }
}
