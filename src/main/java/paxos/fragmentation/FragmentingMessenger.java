package paxos.fragmentation;

import paxos.CommLayer;
import paxos.UDPMessenger;
import paxos.Member;
import paxos.PaxosUtils;

import java.io.Serializable;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class FragmentingMessenger implements CommLayer, UDPMessenger.MessageListener {

    public static final int FRAGMENT_SIZE = 64000;
    private final Map<Long, FragmentCollector> collectors = new HashMap<Long, FragmentCollector>();
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
        } else {
            throw new RuntimeException("Received " + message.getClass());
        }

    }

    private void collectFragment(MessageFragment messageFragment) {
        FragmentCollector collector = getOrCreateCollector(messageFragment);
        collector.addPart(messageFragment.fragmentNo, messageFragment.part);

        if (collector.isComplete()) {
            collectors.remove(messageFragment.id);
            if (upstreamListener != null) upstreamListener.receive(collector.extractMessage());
        }
    }

    private FragmentCollector getOrCreateCollector(MessageFragment messageFragment) {
        if (!collectors.containsKey(messageFragment.id)) {
            collectors.put(messageFragment.id, new FragmentCollector(messageFragment.totalFragments));
        }
        return collectors.get(messageFragment.id);
    }
}