package paxos.communication;

import paxos.GroupMembership;

import java.util.Arrays;
import java.util.List;

public class Members {
    private final List<Member> members;

    public Members(Member... members) {
        this(Arrays.asList(members));
    }

    public Members(List<Member> members) {
        this.members = members;
    }

    public GroupMembership get(int i) {
        return new GroupMembership(members, i);
    }
}
