package paxos;

import java.util.List;
import java.util.Set;

public interface FailureListener {
    void memberFailed(Member member, Set<Member> aliveMembers);
}
