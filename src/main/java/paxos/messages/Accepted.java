package paxos.messages;

import paxos.Member;

import java.io.Serializable;
import java.util.Set;

public class Accepted implements SpecialMessage, MessageWithSender {
    public long viewNo;
    public long seqNo;
    public long msgId;
    public Set<Long> missingSuccess;
    public Member sender;

    public Accepted(long viewNo, long seqNo, long msgId, Set<Long> missingSuccess, Member me) {
        this.viewNo = viewNo;
        this.seqNo = seqNo;
        this.msgId = msgId;
        this.missingSuccess = missingSuccess;
        sender = me;
    }

    public MessageType getMessageType() {
        return MessageType.ACCEPTED;
    }

    @Override
    public String toString() {
        return "ACCEPTED " + msgId + " missing("+missingSuccess+") from " + sender;
    }

    public Member getSender() {
        return sender;
    }
}
