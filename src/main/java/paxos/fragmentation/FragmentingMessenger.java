package paxos.fragmentation;

import paxos.Member;
import paxos.Messenger;

import java.io.IOException;
import java.io.Serializable;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

public class FragmentingMessenger implements Messenger {

    public static final int FRAGMENT_SIZE = 64000;
    private final Messenger messenger;
    private final Map<Long, FragmentCollector> collectors = new HashMap<Long, FragmentCollector>();
    private final BlockingDeque<Serializable> completedMessages = new LinkedBlockingDeque<Serializable>();
    private final int myPositionInGroup;
    private AtomicLong msgIdGen = new AtomicLong(0);


    private boolean running = true;

    public FragmentingMessenger(Messenger msngr) throws SocketException, UnknownHostException {
        this.messenger = msngr;
        this.myPositionInGroup = messenger.getPositionInGroup();

        // get stuff from the udp messenger and collects the fragments
        new Thread() {
            @Override
            public void run() {
                try {
                    while (running) {
                        Serializable message = messenger.receive();

                        if (message instanceof MessageFragment) {
                            collectFragment((MessageFragment) message);
                        } else {
                            throw new RuntimeException("Received " + message.getClass());
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            private void collectFragment(MessageFragment messageFragment) {
                if (!collectors.containsKey(messageFragment.id)) {
                    collectors.put(messageFragment.id, new FragmentCollector(messageFragment.totalFragments));
                }
                FragmentCollector collector = collectors.get(messageFragment.id);
                collector.addPart(messageFragment.fragmentNo, messageFragment.part);

                if (collector.isComplete()) {
                    collectors.remove(messageFragment.id);
                    enqueue(collector.extractMessage());
                }
            }
        }.start();
    }

    private void enqueue(Serializable message) {
        try {
            completedMessages.putLast(message);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Serializable dequeue() {
        try {
            return completedMessages.takeFirst();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public Serializable receive() throws IOException, ClassNotFoundException {
        return dequeue();
    }

    public Member getUID() throws UnknownHostException {
        return messenger.getUID();
    }

    public void sendToAll(Serializable message) {
        MessageFragment[] messageFragments = FragmentationUtils.performFragmentation(message, createMsgId(message), FRAGMENT_SIZE);
        for (MessageFragment messageFragment : messageFragments) {
            messenger.sendToAll(messageFragment);
        }
    }

    public void send(Serializable message, Member member) {
        MessageFragment[] messageFragments = FragmentationUtils.performFragmentation(message, createMsgId(message), FRAGMENT_SIZE);
        for (MessageFragment messageFragment : messageFragments) {
            messenger.send(messageFragment, member);
        }
    }

    private long createMsgId(Serializable message) {
        return myPositionInGroup * 1000000l + msgIdGen.incrementAndGet();
    }


    public int groupSize() {
        return messenger.groupSize();
    }

    public List<Member> getMembers() {
        return messenger.getMembers();
    }

    public void close() {
        this.running = false;
        this.messenger.close();
    }

    public int getPositionInGroup() {
        return messenger.getPositionInGroup();
    }
}
