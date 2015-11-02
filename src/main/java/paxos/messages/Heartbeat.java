package paxos.messages;

import paxos.communication.Member;

import java.io.Serializable;

public class Heartbeat implements Serializable {
    public Member sender;

    public Heartbeat(Member sender) {
        this.sender = sender;
    }

    @Override
    public String toString() {
        return "heartbeat";
    }
}
