package paxos.fragmentation;

import paxos.Group;
import paxos.Messenger;
import paxos.PaxosUtils;
import paxos.Receiver;
import sun.plugin2.message.Message;

import java.io.IOException;
import java.io.Serializable;

public class FragmentingGroup {

    public static final double FRAGMENT_SIZE = 64000;

    private final Group group;

    public FragmentingGroup(Messenger messenger, Receiver receiver) throws IOException {
        this(new Group(messenger, new JoinerReceiver(receiver)));
    }

    FragmentingGroup(Group group) throws IOException {
        this.group = group;
    }

    public void broadcast(Serializable message) throws IOException {
        MessageFragment[] fragments = performFragmentation(message, message.hashCode());
        for (int i = 0; i < fragments.length; i++) {
            group.broadcast(fragments);
        }
    }

    private MessageFragment[] performFragmentation(Serializable message, long msgId) {
        byte[] bytes = PaxosUtils.serialize(message);
        MessageFragment[] fragments = new MessageFragment[(int) Math.ceil(bytes.length/FRAGMENT_SIZE)];
        for (int i = 0; i < fragments.length; i++) {
            int offset = (int) (i*FRAGMENT_SIZE);
            int remainingBytes = bytes.length - offset;
            int fragmentLength = (int) Math.min(remainingBytes, FRAGMENT_SIZE);
            byte[] fragmentBytes = new byte[fragmentLength];
            System.arraycopy(bytes, offset, fragmentBytes, 0, fragmentLength);
            fragments[i] = new MessageFragment(msgId, fragmentBytes, i, fragments.length);
        }
        return fragments;
    }

    private static class JoinerReceiver implements Receiver {
        private final Receiver receiver;

        public JoinerReceiver(Receiver receiver) {
            this.receiver = receiver;
        }

        public void receive(Serializable messsage) {

        }
    }

    private static class MessageFragment implements Serializable {
        private long id;
        private byte[] part;
        private int fragmentNo;
        private int totalFragments;

        public MessageFragment(long id, byte[] part, int fragmentNo, int totalFragments) {
            this.id = id;
            this.part = part;
            this.fragmentNo = fragmentNo;
            this.totalFragments = totalFragments;
        }
    }
}
