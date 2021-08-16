package paxos;

import paxos.communication.CommLayer;
import paxos.communication.Member;
import paxos.messages.*;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents the behaviours of non-leaders of the group. The leader will perform this logic as well.
 * An acceptor can receive these messages:
 * <ul>
 *  <li>NEW_VIEW: a member is asking to become the leader</li>
 *  <li>ACCEPT: the leader (or a member thinking it is a leader) asks members to accept a message</li>
 *  <li>SUCCESS: the leader is telling us that a majority of members have accepted the message</li>
 * </ul>
 */
public class AcceptorLogic {
    public static final long MAX_CIRCULATING_MESSAGES = 1000000l;
    private final GroupMembership membership;
    private final CommLayer messenger;
    private final BufferedReceiver receiver;
    private final Member me;
    private final WaitingRoom waitingForResponse = new WaitingRoom();
    private final int myPositionInGroup;

    Map<Long, Acceptance> accepted = new HashMap<Long, Acceptance>(); // what we accepted for each seqNo
    private Member leader;
    private long viewNumber;
    private MissingMessagesTracker missing = new MissingMessagesTracker(); // missing SUCCESS messages
    private AtomicLong msgIdGen = new AtomicLong(0);


    public AcceptorLogic(GroupMembership membership, CommLayer messenger, Receiver receiver) {
        this.membership = membership;
        this.messenger = messenger;
        this.receiver = new BufferedReceiver(receiver);
        this.me = membership.getUID();
        this.myPositionInGroup = membership.getPositionInGroup();
        this.leader = me;
    }

    /**
     * Invoked when a client wants to send a message to the group. Regardless of whether this member is the leader,
     * a message will be sent to the leader to request a consensus algorithm to be initiated and the thread will block until
     * the consensus completes.
     *
     * @param message the message to be broadcast
     */
    public void broadcast(Serializable message) {
        long msgId = createMsgId(message);
        boolean broadcastSuccessful = false;
        try {
            while (!broadcastSuccessful) {
//                System.out.println("sending request to " + leader);
                messenger.sendTo(leader, PaxosUtils.serialize(new BroadcastRequest(message, msgId)));
                broadcastSuccessful = waitingForResponse.waitALittle(msgId);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private long createMsgId(Serializable message) {
        return myPositionInGroup * MAX_CIRCULATING_MESSAGES + msgIdGen.incrementAndGet() % MAX_CIRCULATING_MESSAGES;
    }

    /**
     * Invoked when a message is received from a group member (could be oneself).
     *
     * @param message
     */
    public void dispatch(Serializable message) {
        if (message instanceof SpecialMessage) {
            SpecialMessage specialMessage = (SpecialMessage) message;
            switch (specialMessage.getMessageType()) {
                case NEW_VIEW: onNewView((NewView) specialMessage); break;
                case ACCEPT: onAccept((Accept) specialMessage); break;
                case SUCCESS: onSuccess((Success) specialMessage); break;
            }
        }
    }

    /**
     * A members is asking us to vote for him as the new leader. We will accept only if his viewNumber is higher than
     * the current one.
     *
     * @param newView
     */
    private void onNewView(NewView newView) {
        if (newView.viewNumber > viewNumber) {
            System.out.println(me + ": setting leader to " + newView.leader);
            this.leader = newView.leader;
            this.viewNumber = newView.viewNumber;
            messenger.sendTo(leader, PaxosUtils.serialize(new ViewAccepted(viewNumber, accepted, me)));
        } else if (newView.viewNumber == viewNumber && newView.leader.equals(leader)) {
            messenger.sendTo(leader, PaxosUtils.serialize(new ViewAccepted(viewNumber, accepted, me)));
        }
    }

    /**
     * A member that believes to be a leader is asking us to accept a message. If it is an old leader, we will reject.
     *
     * @param accept
     */
    private void onAccept(Accept accept) {
        if (accept.viewNo < viewNumber) {
            messenger.sendTo(accept.sender, PaxosUtils.serialize(new Abort(accept.viewNo, accept.seqNo)));
        } else {
            accepted.put(accept.seqNo, new Acceptance(accept.viewNo, accept.message, accept.msgId));
            messenger.sendTo(accept.sender, PaxosUtils.serialize(new Accepted(accept.viewNo, accept.seqNo, accept.msgId, missing.getMissing(accept.seqNo), me)));
        }
    }

    /**
     * The leader is telling us that a majority has accepted a message. If there is a client waiting for consensus to
     * complete, we unblock it. We also tell the leader that we got the success message, so it can do garbage collection.
     *
     * @param success
     */
    private void onSuccess(Success success) {
        receiver.receive(success.seqNo, success.message);
        missing.received(success.seqNo);
        waitingForResponse.unblock(success.msgId);
        messenger.sendTo(leader, PaxosUtils.serialize(new SuccessAck(success.msgId, me)));
    }
}
