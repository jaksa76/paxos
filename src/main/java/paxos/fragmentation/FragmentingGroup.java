package paxos.fragmentation;

import paxos.Group;
import paxos.Messenger;
import paxos.PaxosUtils;
import paxos.Receiver;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

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
            group.broadcast(fragments[i]);
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

        static class FragmentCollector {
            private final byte[][] parts;
            private int partsReceived = 0;

            public FragmentCollector(int parts) {
                this.parts = new byte[parts][];
            }

            public void addPart(int partNo, byte[] bytes) {
                parts[partNo] = bytes;
                partsReceived++;
            }

            public boolean isComplete() {
                return partsReceived == parts.length;
            }

            public Serializable extractMessage() {
                try {
                    int totalBytes = 0;
                    for (int i = 0; i < parts.length; i++) totalBytes += parts[i].length;

                    byte[] concatenated = new byte[totalBytes];
                    int cursor = 0;
                    for (int i = 0; i < parts.length; i++) {
                        System.arraycopy(parts[i], 0, concatenated, cursor, parts[i].length);
                        cursor += parts[i].length;
                    }

                    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(concatenated));
                    return (Serializable) ois.readObject();
                } catch (Exception e) {
                    throw new RuntimeException("Could not deserialize concatenated message", e);
                }
            }
        }
    }

    static class MessageFragment implements Serializable {
        long id;
        byte[] part;
        int fragmentNo;
        int totalFragments;

        public MessageFragment(long id, byte[] part, int fragmentNo, int totalFragments) {
            this.id = id;
            this.part = part;
            this.fragmentNo = fragmentNo;
            this.totalFragments = totalFragments;
        }

        @Override
        public String toString() {
            return "message fragment " + (fragmentNo+1) + "/" + totalFragments + " len: " + part.length;
        }
    }
}
