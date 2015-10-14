package paxos;

import java.util.List;

public interface CommLayer {
    void setListener(MessageListener listener);
    void sendTo(List<Member> members, byte[] message);
    void sendTo(Member member, byte[] message);
    void close();

    interface MessageListener {
        void receive(byte[] message);
    }
}
