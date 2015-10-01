package paxos.fragmentation;

import java.io.Serializable;

class MessageFragment implements Serializable {
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
