package paxos.dynamic;

import paxos.communication.Member;

import java.io.Serializable;
import java.util.List;

// TODO merge with GroupChange???
class GroupInfo implements Serializable {
    final long groupId;
    final List<Member> members;

    public GroupInfo(List<Member> members, long groupId) {
        this.members = members;
        this.groupId = groupId;
    }
}
