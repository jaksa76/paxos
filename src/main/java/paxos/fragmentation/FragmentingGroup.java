package paxos.fragmentation;

import paxos.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class FragmentingGroup {

    public static final int FRAGMENT_SIZE = 64000;

    private final Group group;
    private final int myPositionInGroup;
    private AtomicLong msgIdGen = new AtomicLong(0);


    public FragmentingGroup(GroupMembership membership, CommLayer layer, Receiver receiver) {
        this.group = new Group(membership, new FragmentingMessenger(layer), new JoinerReceiver(receiver));
        this.myPositionInGroup = membership.getPositionInGroup();
    }

    // for testing
    FragmentingGroup(Group group, int position) {
        this.group = group;
        this.myPositionInGroup = position;
    }


    public void broadcast(Serializable message) throws IOException {
        long messageId = createMsgId(message);
        byte[] bytes = PaxosUtils.serialize(message);
        MessageFragment[] fragments = FragmentationUtils.performFragmentation(bytes, messageId, FRAGMENT_SIZE);
        for (MessageFragment fragment : fragments) {
            group.broadcast(fragment);
        }
    }

    public void close() {
        group.close();
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

        public void receive(Serializable message) {
            if (message instanceof MessageFragment) {
                MessageFragment messageFragment = (MessageFragment) message;
                if (!collectors.containsKey(messageFragment.id)) {
                    collectors.put(messageFragment.id, new FragmentCollector(messageFragment.totalFragments));
                }
                FragmentCollector collector = collectors.get(messageFragment.id);
                collector.addPart(messageFragment.fragmentNo, messageFragment.part);

                if (collector.isComplete()) {
                    collectors.remove(messageFragment.id);
                    if (receiver != null) receiver.receive((Serializable) PaxosUtils.deserialize(collector.extractMessage()));
                }
            }
        }
    }
}
