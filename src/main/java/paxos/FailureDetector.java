package paxos;

import paxos.communication.CommLayer;
import paxos.communication.Member;
import paxos.communication.Tick;
import paxos.messages.Heartbeat;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class FailureDetector {
    private static final long INTERVAL = 1000; // 1 second
    private static final long TIMEOUT = 3000; // 3 seconds

    private final GroupMembership membership;
    private final CommLayer messenger;
    private final FailureListener listener;
    private final Map<Member, Long> lastHeardFrom = new ConcurrentHashMap<Member, Long>();
    private final byte[] heartbeat;

    private Set<Member> membersAlive = new HashSet<Member>();
    private long lastHearbeat = 0;
    private long time = 0;

//    public FailureDetector(GroupMembership membership, CommLayer messenger, FailureListener listener) {
//        this(membership, messenger, listener, System.currentTimeMillis());
//    }

    // for testing purposes
    public FailureDetector(final GroupMembership membership, final CommLayer messenger, final FailureListener listener) {
        this.membership = membership;
        this.messenger = messenger;
        this.listener = listener;

        membersAlive.addAll(membership.getMembers());

        heartbeat = PaxosUtils.serialize(new Heartbeat(membership.getUID()));
    }

    private void sendHeartbeat(long time) {
        messenger.sendTo(membership.getMembers(), heartbeat);
        this.lastHearbeat = time;
    }

    private void checkForFailedMembers(long time) {
        for (Member member : membership.getMembers()) {
            if (member.equals(membership.getUID())) continue;
            if (!lastHeardFrom.containsKey(member)) initialize(time, member); // lazy initialization sinse we don't have the initial time
            if (time - lastHeardFrom.get(member) > TIMEOUT) {
                if (membersAlive.contains(member)) {
                    membersAlive.remove(member);
                    listener.memberFailed(member, membersAlive);
                }
            } else {
                if (!membersAlive.contains(member)) {
                    membersAlive.add(member);
                    // TODO notify member recovered
                }
            }
        }
    }

    private Long initialize(long time, Member member) {
        return lastHeardFrom.put(member, time);
    }

    public void update(long time) {
        this.time = time;
        if (lastHearbeat + INTERVAL < time) sendHeartbeat(time);
        checkForFailedMembers(time);
    }

    public void dispatch(Serializable message) {
        if (message instanceof Heartbeat) {
            Heartbeat heartbeat = (Heartbeat) message;
            lastHeardFrom.put(heartbeat.sender, time);
        } else if (message instanceof Tick) {
            update(((Tick) message).time);
        }
    }
}
