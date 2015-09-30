package paxos;

import java.io.IOException;
import java.io.Serializable;

public interface Receiver {
    void receive(Serializable messsage);
}
