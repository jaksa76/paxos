package paxos.fragmentation;

import paxos.PaxosUtils;

import java.io.Serializable;

public class FragmentationUtils {
    public static MessageFragment[] performFragmentation(byte[] message, long msgId, int size) {
        byte[][] byteFragments = performFragmentation(message, size);
        MessageFragment[] fragments = new MessageFragment[byteFragments.length];
        for (int i = 0; i < fragments.length; i++) {
            fragments[i] = new MessageFragment(msgId, byteFragments[i], i, fragments.length);
        }
        return fragments;
    }

    public static byte[][] performFragmentation(byte[] bytes, int size) {
        byte[][] fragments = new byte[(int) Math.ceil(bytes.length/((double)size))][];
        for (int i = 0; i < fragments.length; i++) {
            int offset = i*size;
            int remainingBytes = bytes.length - offset;
            int fragmentLength = Math.min(remainingBytes, size);
            fragments[i] = new byte[fragmentLength];
            System.arraycopy(bytes, offset, fragments[i], 0, fragmentLength);
        }
        return fragments;
    }
}
