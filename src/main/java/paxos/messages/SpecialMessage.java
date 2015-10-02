package paxos.messages;

import sun.plugin2.message.Message;

import java.io.Serializable;

public interface SpecialMessage extends Serializable {
    MessageType getMessageType();

    enum MessageType {
        BROADCAST_REQ,
        BROADCAST,
        ACCEPT,
        ACCEPTED,
        JOIN,
        NEW_VIEW,
        SUCCESS,
        ABORT,
        PREVIOUS_OUTCOME,
        SUCCESS_ACK, VIEW_ACCEPTED
    }
}
