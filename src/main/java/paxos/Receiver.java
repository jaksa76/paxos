package paxos;

import java.io.Serializable;

public interface Receiver {
    void receive(Serializable messsage);
}
