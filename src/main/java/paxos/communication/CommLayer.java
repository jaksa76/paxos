package paxos.communication;

import java.util.List;

public interface CommLayer {
    void sendTo(List<Member> members, byte[] message);
    void sendTo(Member member, byte[] message);
    void setListener(MessageListener listener);
    void close();

    interface MessageListener {
        void receive(byte[] message);
    }
}
