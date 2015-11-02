package paxos.dynamic;

import paxos.communication.Member;

import java.io.Serializable;
import java.util.List;

public class GroupChange implements Serializable {
    public final List<Member> members;
    public final long groupId;

    public GroupChange(long groupId, List<Member> members) {
        this.members = members;
        this.groupId = groupId;
    }
}
