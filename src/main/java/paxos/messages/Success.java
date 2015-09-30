package paxos.messages;

import java.io.Serializable;

public class Success implements SpecialMessage {
    public long seqNo;
    public Serializable message;
    public long msgId;

    public Success(long seqNo, Serializable message, long msgId) {
        this.seqNo = seqNo;
        this.message = message;
        this.msgId = msgId;
    }

    @Override
    public String toString() {
        return "SUCCESS " + seqNo + " " + msgId + "(" + message + ")";
    }

    public MessageType getMessageType() {
        return MessageType.SUCCESS;
    }
}
