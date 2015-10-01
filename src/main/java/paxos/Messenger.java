package paxos;

import java.io.IOException;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.List;

public interface Messenger {
    Serializable receive() throws IOException, ClassNotFoundException;

    Member getUID() throws UnknownHostException;

    void sendToAll(Serializable message);

    void send(Serializable message, Member member);

    int groupSize();

    List<Member> getMembers();

    void close();

    int getPositionInGroup();
}
