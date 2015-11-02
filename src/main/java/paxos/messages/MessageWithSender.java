package paxos.messages;

import paxos.communication.Member;

import java.io.Serializable;

public interface MessageWithSender extends Serializable {
    Member getSender();
}
