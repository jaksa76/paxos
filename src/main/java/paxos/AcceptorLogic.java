package paxos;

import paxos.messages.*;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class AcceptorLogic {
    private final Messenger messenger;
    private final BufferedReceiver receiver;
    private final Member me;
    private final WaitingRoom waitingForResponse = new WaitingRoom();
    private final int myPositionInGroup;

    Map<Long, Acceptance> accepted = new HashMap<Long, Acceptance>(); // what we accepted for each seqNo
    private Member leader;
    private long viewNumber;
    private long tail = -1; // we have received SUCCESS for all messages before tail
    private Set<Long> undelivered = new HashSet<Long>(); // undeliverd SUCCESSFULL messages
    private AtomicLong msgIdGen = new AtomicLong(0);


    public AcceptorLogic(Messenger Messenger, Receiver receiver) throws UnknownHostException {
        this.messenger = Messenger;
        this.receiver = new BufferedReceiver(receiver);
        this.me = messenger.getUID();
        this.myPositionInGroup = PaxosUtils.findPositionInGroup(me, messenger.getMembers());
    }

    public void broadcast(Serializable message) {
        long msgId = createMsgId(message);
        boolean broadcastSuccessfull = false;
        try {
            while (!broadcastSuccessfull) {
//                System.out.println("sending request to " + leader);
                messenger.send(new BroadcastRequest(message, msgId), leader);
                broadcastSuccessfull = waitingForResponse.waitALittle(msgId);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private long createMsgId(Serializable message) {
        return myPositionInGroup * 1000000l + msgIdGen.incrementAndGet();
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
            messenger.send(new ViewAccepted(viewNumber, accepted, me), leader);
        } else if (newView.viewNumber == viewNumber && newView.leader.equals(leader)) {
            messenger.send(new ViewAccepted(viewNumber, accepted, me), leader);
        }
    }

    private void onAccept(Accept accept) {
        if (accept.viewNo < viewNumber) {
            messenger.send(new Abort(accept.viewNo, accept.seqNo), accept.sender);
        } else {
            accepted.put(accept.seqNo, new Acceptance(accept.viewNo, accept.message, accept.msgId));
            messenger.send(new Accepted(accept.viewNo, accept.seqNo, accept.msgId, getMissingSuccess(accept.seqNo), me), accept.sender);
        }
    }

    private void onSuccess(Success success) {
        receiver.receive(success.seqNo, success.message);
        updateTail(success);
        waitingForResponse.unblock(success.msgId);
        messenger.send(new SuccessAck(success.msgId, me), leader);
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
