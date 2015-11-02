package paxos;

import paxos.communication.Member;

import java.util.Set;

public interface FailureListener {
    void memberFailed(Member member, Set<Member> aliveMembers);
}
