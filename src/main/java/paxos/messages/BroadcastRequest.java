package paxos.messages;

import java.io.Serializable;

/**
 * Sent to the leader to ask the leader to start a broadcast protocol for the given message.
 */
public class BroadcastRequest implements SpecialMessage {
    public Serializable message;
    public long msgId;

    public BroadcastRequest(Serializable message, long msgId) {
        this.message = message;
        this.msgId = msgId;
    }

    public MessageType getMessageType() {
        return MessageType.BROADCAST_REQ;
    }
}
