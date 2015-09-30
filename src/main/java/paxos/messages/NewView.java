package paxos.messages;

import java.io.Serializable;
import java.util.List;

import paxos.Member;

public class NewView implements SpecialMessage {
    public final Member leader;
    public final long viewNumber;

    public NewView(Member leader, long viewNumber) {
        this.leader = leader;
        this.viewNumber = viewNumber;
    }

    @Override
    public String toString() {
        return "NEW_VIEW " + leader.toString() + "(" + viewNumber + ")";
    }

    public MessageType getMessageType() {
        return MessageType.NEW_VIEW;
    }
}
