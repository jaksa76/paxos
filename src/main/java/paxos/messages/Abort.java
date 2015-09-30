package paxos.messages;

import java.io.Serializable;

public class Abort implements SpecialMessage {
    public final long viewNo;
    public final long seqNo;

    public Abort(long viewNo, long seqNo) {
        this.viewNo = viewNo;
        this.seqNo = seqNo;
    }

    public MessageType getMessageType() {
        return MessageType.ABORT;
    }
}
