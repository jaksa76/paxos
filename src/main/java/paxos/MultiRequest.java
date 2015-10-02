package paxos;

import paxos.messages.Heartbeat;
import paxos.messages.MessageWithSender;
import paxos.messages.SpecialMessage;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class MultiRequest<T extends Serializable, R extends MessageWithSender> {
    public static final int RESEND_INTERVAL = 1000;
    protected final Messenger messenger;
    protected final T req;
    protected Map<Member, R> responses = new HashMap<Member, R>();
    private long lastResend = 0;
    private boolean finished = false;
    private boolean quorumHasBeenReached = false;
    private boolean allMembersHaveReplied = false;

    public MultiRequest(Messenger messenger, T req) {
        this(messenger, req, System.currentTimeMillis());
    }

    public MultiRequest(Messenger messenger, T req, long time) {
        this.messenger = messenger;
        this.req = req;
        messenger.sendToAll(req);
        this.lastResend = time;
    }

    /**
     * Override this to filter responses.
     *
     * @param message the message received
     * @return the message if this is a response, <code>null</code> otherwise
     */
    protected R filterResponse(Serializable message) {
        try {
            return (R) message;
        } catch (ClassCastException e) {
            return null;
        }
    }

    /**
     * Invoked when a quorum of members have replied.
     */
    protected void onQuorumReached() {}

    /**
     * Invoked when all the members have replied.
     */
    protected void onCompleted() {
        finish();
    }

    /**
     * Invoked periodically. By default it will resend the request to
     * the members that haven't replied.
     *
     * @param time - current time in milliseconds
     */
    public void tick(long time) {
        if (time > lastResend + RESEND_INTERVAL) resendRequests(time);
    }

    /**
     * You can override this if you want to define a different quorum.
     *
     * @return true if we have received responses from a quorum of members.
     */
    protected boolean haveQuorum() {
        return responses.size() > messenger.groupSize() / 2;
    }

    /**
     * We can forget everything about this req-resp.
     */
    final protected void finish() {
        this.finished = true;
    }

    public boolean isFinished() {
        return finished;
    }

    /**
     * All messages are forwarded to this method.
     *
     * @param message
     */
    final public void receive(Serializable message) {
        R resp = filterResponse(message);
        if (resp != null) {
            responses.put(resp.getSender(), resp);
            if (haveQuorum() && !quorumHasBeenReached) {
                onQuorumReached();
                quorumHasBeenReached = true;
            }
            if (allMembersReplied() && !allMembersHaveReplied) {
                onCompleted();
                allMembersHaveReplied = true;
            }
        }
    }

    protected void resendRequests(long time) {
        for (Member member : messenger.getMembers()) {
            if (!responses.containsKey(member)) messenger.send(req, member);
        }
        lastResend = time;
    }

    protected boolean allMembersReplied() {
        return responses.size() == messenger.groupSize();
    }
}
