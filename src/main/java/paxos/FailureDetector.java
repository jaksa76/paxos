package paxos;

import paxos.messages.Heartbeat;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class FailureDetector {
    private static final long INTERVAL = 1000; // 1 second
    private static final long TIMEOUT = 3000; // 3 seconds
    private final GroupMembership membership;
    private final CommLayer messenger;
    private final FailureListener listener;
    private final Map<Member, Long> lastHeardFrom = new ConcurrentHashMap<Member, Long>();
    private final Thread heart;
    private boolean running = true;
    private Set<Member> membersAlive = new HashSet<Member>();
    private final TimeProvider timeProvider;

    public FailureDetector(GroupMembership membership, CommLayer messenger, FailureListener listener) {
        this(membership, messenger, listener, new DefaultTimeProvider());
    }

    // for testing purposes
    FailureDetector(final GroupMembership membership, final CommLayer messenger, final FailureListener listener, final TimeProvider timeProvider) {
        this.membership = membership;
        this.messenger = messenger;
        this.listener = listener;
        this.timeProvider = timeProvider;

        for (Member member : membership.getMembers()) {
            lastHeardFrom.put(member, timeProvider.getTime());
            membersAlive.add(member);
        }

        final byte[] heartbeat = PaxosUtils.serialize(new Heartbeat(membership.getUID()));
        this.heart = new Thread() {
            @Override public void run() {
                try {
                    while (running) {
                        messenger.sendTo(membership.getMembers(), heartbeat);
                        Thread.sleep(INTERVAL);
                        for (Member member : membership.getMembers()) {
                            if (timeProvider.getTime() - lastHeardFrom.get(member) > TIMEOUT) {
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
                } catch (InterruptedException e) {
                    // we're closing
                }
            }
        };
        heart.start();
    }

    public void dispatch(Serializable message) {
        if (message instanceof Heartbeat) {
            Heartbeat heartbeat = (Heartbeat) message;
            lastHeardFrom.put(heartbeat.sender, timeProvider.getTime());
        }
    }

    public void close() {
        this.running = false;
        this.heart.interrupt();
    }
}
