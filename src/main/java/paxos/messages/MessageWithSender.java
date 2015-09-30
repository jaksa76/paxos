package paxos.messages;

import paxos.Member;

import java.io.Serializable;

public interface MessageWithSender extends Serializable {
    public Member getSender();
}
