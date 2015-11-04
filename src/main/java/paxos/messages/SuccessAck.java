package paxos.messages;

import paxos.communication.Member;

/**
 * Sent by the members to the leader to confirm the receving of the {@link Success} message so
 * the leader can do its garbage collection.
 */
public class SuccessAck implements MessageWithSender, SpecialMessage {
    private final long msgId;
    private Member sender;

    public SuccessAck(long msgId, Member sender) {
        this.msgId = msgId;
        this.sender = sender; }

    public Member getSender() { return sender; }

    public long getMsgId() {
        return msgId;
    }

    public MessageType getMessageType() {
        return MessageType.SUCCESS_ACK;
    }

    @Override
    public String toString() {
        return "SUCCESS_ACK " + msgId + " " + sender.toString();
    }
}
