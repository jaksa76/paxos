package paxos.messages;

import paxos.communication.Member;

import java.io.Serializable;

/**
 * Used to detect failed processes
 */
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
