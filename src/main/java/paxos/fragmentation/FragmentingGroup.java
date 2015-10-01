package paxos.fragmentation;

import paxos.Group;
import paxos.Messenger;
import paxos.PaxosUtils;
import paxos.Receiver;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class FragmentingGroup {

    public static final int FRAGMENT_SIZE = 64000;

    private final Group group;
    private final int myPositionInGroup;
    private AtomicLong msgIdGen = new AtomicLong(0);

    public FragmentingGroup(Messenger messenger, Receiver receiver) throws IOException {
        this(new Group(new FragmentingMessenger(messenger), new JoinerReceiver(receiver)));
    }

    FragmentingGroup(Group group) throws IOException {
        this.group = group;
        this.myPositionInGroup = group.getPositionInGroup();
    }

    public void broadcast(Serializable message) throws IOException {
        long messageId = createMsgId(message);
        MessageFragment[] fragments = FragmentationUtils.performFragmentation(message, messageId, FRAGMENT_SIZE);
        for (int i = 0; i < fragments.length; i++) {
            group.broadcast(fragments[i]);
        }
    }

    private long createMsgId(Serializable message) {
        return myPositionInGroup * 1000000l + msgIdGen.incrementAndGet();
    }

    static class JoinerReceiver implements Receiver {
        private final Receiver receiver;
        private Map<Long, FragmentCollector> collectors = new HashMap<Long, FragmentCollector>();

        public JoinerReceiver(Receiver receiver) {
            this.receiver = receiver;
        }

        public void receive(Serializable messsage) {
            if (messsage instanceof MessageFragment) {
                MessageFragment messageFragment = (MessageFragment) messsage;
                if (!collectors.containsKey(messageFragment.id)) {
                    collectors.put(messageFragment.id, new FragmentCollector(messageFragment.totalFragments));
                }
                FragmentCollector collector = collectors.get(messageFragment.id);
                collector.addPart(messageFragment.fragmentNo, messageFragment.part);

                if (collector.isComplete()) {
                    collectors.remove(messageFragment.id);
                    receiver.receive(collector.extractMessage());
                }
            }
        }

    }

}
