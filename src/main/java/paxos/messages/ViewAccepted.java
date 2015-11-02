package paxos.messages;

import paxos.Acceptance;
import paxos.communication.Member;

import java.util.Map;

public class ViewAccepted implements SpecialMessage, MessageWithSender {
    public final long viewNumber;
    public final Map<Long, Acceptance> accepted;
    public final Member sender;

    public ViewAccepted(long viewNumber, Map<Long, Acceptance> accepted, Member sender) {
        this.viewNumber = viewNumber;
        this.accepted = accepted;
        this.sender = sender;
    }

    public MessageType getMessageType() {
        return MessageType.VIEW_ACCEPTED;
    }

    public Member getSender() {
        return sender;
    }

    @Override
    public String toString() {
        return "VIEW_ACCEPTED " + viewNumber + " " + sender;
    }
}
