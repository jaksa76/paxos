package paxos.fragmentation;

import paxos.communication.CommLayer;
import paxos.communication.Tick;
import paxos.communication.UDPMessenger;
import paxos.communication.Member;
import paxos.PaxosUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This communication layer fragments messages that cannot
 */
public class FragmentingMessenger implements CommLayer, UDPMessenger.MessageListener {

    public static final int FRAGMENT_SIZE = 64000;
    private final MessageReconstructor messageReconstructor = new MessageReconstructor();
    private final CommLayer messenger;
    private UDPMessenger.MessageListener upstreamListener;
    private AtomicLong msgIdGen = new AtomicLong(0);

    public FragmentingMessenger(CommLayer messenger) {
        this.messenger = messenger;
        this.messenger.setListener(this);
    }

    public void setListener(MessageListener listener) {
        this.upstreamListener = listener;
    }

    public void sendTo(List<Member> members, byte[] message) {
        MessageFragment[] messageFragments = FragmentationUtils.performFragmentation(message, createMsgId(message), FRAGMENT_SIZE);
        for (MessageFragment messageFragment : messageFragments) {
            messenger.sendTo(members, PaxosUtils.serialize(messageFragment));
        }
    }

    public void sendTo(Member member, byte[] message) {
        MessageFragment[] messageFragments = FragmentationUtils.performFragmentation(message, createMsgId(message), FRAGMENT_SIZE);
        for (MessageFragment messageFragment : messageFragments) {
            messenger.sendTo(member, PaxosUtils.serialize(messageFragment));
        }
    }

    private long createMsgId(Serializable message) {
        return (long) (Math.random() * Long.MAX_VALUE);
    }

    public void close() {
        this.messenger.close();
    }

    public void receive(byte[] bytes) {
        Serializable message = (Serializable) PaxosUtils.deserialize(bytes);

        if (message instanceof MessageFragment) {
            collectFragment((MessageFragment) message);
        } else if (message instanceof Tick) {
            if (upstreamListener != null) upstreamListener.receive(bytes);
        } else {
            throw new RuntimeException("Received " + message.getClass());
        }

    }

    private void collectFragment(MessageFragment messageFragment) {
        byte[] completeMsg = messageReconstructor.collectFragment(messageFragment);
        if (completeMsg != null && upstreamListener != null) upstreamListener.receive(completeMsg);
    }
}
