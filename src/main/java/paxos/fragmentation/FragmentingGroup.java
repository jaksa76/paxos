package paxos.fragmentation;

import paxos.*;
import paxos.communication.CommLayer;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class implements totally ordered reliable broadcast. As opposed to {}@link Group} this class
 * supports messages that are larger than a UDP packet.
 */
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
            // TODO send the fragments in parallel
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
        private final MessageReconstructor messageReconstructor = new MessageReconstructor();

        public JoinerReceiver(Receiver receiver) {
            this.receiver = receiver;
        }

        public void receive(Serializable message) {
            if (message instanceof MessageFragment) {
                collectFragment((MessageFragment) message);
            } else {
                System.out.println("don't know about " + message);
            }
        }

        private void collectFragment(MessageFragment fragment) {
            byte[] completeMsg = messageReconstructor.collectFragment(fragment);
            if (completeMsg != null && receiver != null)
                receiver.receive((Serializable) PaxosUtils.deserialize(completeMsg));
        }
    }
}
