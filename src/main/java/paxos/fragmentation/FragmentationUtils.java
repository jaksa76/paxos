package paxos.fragmentation;

import paxos.PaxosUtils;

import java.io.Serializable;

public class FragmentationUtils {
    public static MessageFragment[] performFragmentation(Serializable message, long msgId, int size) {
        byte[] bytes = PaxosUtils.serialize(message);
        MessageFragment[] fragments = new MessageFragment[(int) Math.ceil(bytes.length/((double)size))];
        for (int i = 0; i < fragments.length; i++) {
            int offset = (int) (i*size);
            int remainingBytes = bytes.length - offset;
            int fragmentLength = (int) Math.min(remainingBytes, size);
            byte[] fragmentBytes = new byte[fragmentLength];
            System.arraycopy(bytes, offset, fragmentBytes, 0, fragmentLength);
            fragments[i] = new MessageFragment(msgId, fragmentBytes, i, fragments.length);
        }
        return fragments;
    }
}
