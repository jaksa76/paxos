package paxos.dynamic;

import paxos.PaxosUtils;

import java.io.Serializable;

public class DynamicGroupMessage implements Serializable {
    public final long groupId;
    public final byte[] message;

    public DynamicGroupMessage(long groupId, byte[] message) {
        this.groupId = groupId;
        this.message = message;
    }
}
