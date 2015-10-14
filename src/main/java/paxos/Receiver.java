package paxos;

import java.io.IOException;
import java.io.Serializable;
import java.net.SocketException;
import java.net.UnknownHostException;

public interface Receiver {
    void receive(Serializable message);
}
