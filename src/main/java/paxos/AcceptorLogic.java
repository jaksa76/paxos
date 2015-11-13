package paxos;

import paxos.communication.CommLayer;
import paxos.communication.Member;
import paxos.messages.*;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

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
    private long tail = -1; // we have received SUCCESS for all messages before tail
    private Set<Long> undelivered = new HashSet<Long>(); // undelivered SUCCESSFUL messages
    private AtomicLong msgIdGen = new AtomicLong(0);


    public AcceptorLogic(GroupMembership membership, CommLayer messenger, Receiver receiver) {
        this.membership = membership;
        this.messenger = messenger;
        this.receiver = new BufferedReceiver(receiver);
        this.me = membership.getUID();
        this.myPositionInGroup = membership.getPositionInGroup();
        this.leader = me;
    }

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

    private void onAccept(Accept accept) {
        if (accept.viewNo < viewNumber) {
            messenger.sendTo(accept.sender, PaxosUtils.serialize(new Abort(accept.viewNo, accept.seqNo)));
        } else {
            accepted.put(accept.seqNo, new Acceptance(accept.viewNo, accept.message, accept.msgId));
            messenger.sendTo(accept.sender, PaxosUtils.serialize(new Accepted(accept.viewNo, accept.seqNo, accept.msgId, getMissingSuccess(accept.seqNo), me)));
        }
    }

    private void onSuccess(Success success) {
        receiver.receive(success.seqNo, success.message);
        updateTail(success);
        waitingForResponse.unblock(success.msgId);
        messenger.sendTo(leader, PaxosUtils.serialize(new SuccessAck(success.msgId, me)));
    }

    private void updateTail(Success success) {
        if (tail == success.seqNo - 1) {
            tail++;
            while (!undelivered.isEmpty()) {
                if (undelivered.contains(tail+1)) {
                    undelivered.remove(tail + 1);
                    tail++;
                } else {
                    break;
                }
            }
        } else {
            undelivered.add(success.seqNo);
        }
    }

    public Set<Long> getMissingSuccess(long seqNo) {
        Set<Long> missingSuccess = new HashSet<Long>();
        for (long i = tail+1; i < seqNo; i++) {
            if (!undelivered.contains(i)) missingSuccess.add(i);
        }
        return missingSuccess;
    }
}
