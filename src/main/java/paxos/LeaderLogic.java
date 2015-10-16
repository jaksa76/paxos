package paxos;

import paxos.messages.*;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.*;

public class LeaderLogic implements FailureListener {
    private static final NoOp NO_OP = new NoOp();
    private final GroupMembership membership;
    private final CommLayer messenger;
    private final TimeProvider timeProvider;
    private final Member me;
    private final Map<Long, Proposal> proposals = new HashMap<Long, Proposal>();
    private final Map<Long, Serializable> successfulMessages = new HashMap<Long, Serializable>();
    private final Map<Long, Long> successfulMsgIds = new HashMap<Long, Long>();
    private final HashSet<Long> messagesCirculating = new HashSet<Long>(); // msgIds of messages that were not
    private final List<MultiRequest> assistants = new LinkedList<MultiRequest>();

    private long viewNumber = 0;
    private long seqNo = 0;
    private boolean iAmElected = false;

    public LeaderLogic(GroupMembership membership, CommLayer commLayer) {
        this(membership, commLayer, new DefaultTimeProvider());
    }

    // for testing
    LeaderLogic(GroupMembership membership, CommLayer commLayer, TimeProvider timeProvider) {
        this.membership = membership;
        this.messenger = commLayer;
        this.timeProvider = timeProvider;
        this.me = membership.getUID();
        Member leader = PaxosUtils.selectLeader(membership.getMembers());
        if (leader.equals(me)) {
            assistants.add(new Election(membership, messenger, timeProvider.getTime(), viewNumber + newViewNumber()));
        }
    }

    public synchronized void dispatch(Serializable message) {
        if (message instanceof SpecialMessage) {
            SpecialMessage specialMessage = (SpecialMessage) message;
            switch (specialMessage.getMessageType()) {
                case ABORT: onAbort((Abort) specialMessage); break;
                case BROADCAST_REQ: onBroadcastRequest((BroadcastRequest) specialMessage); break;
                case NEW_VIEW: onNewView((NewView) specialMessage); break;
            }
        }
        for (MultiRequest assistant : new ArrayList<MultiRequest>(assistants)) {
            assistant.receive(message);
            if (assistant.isFinished()) assistants.remove(assistant);
        }
    }

    public synchronized void update() {
        for (MultiRequest assistant : assistants) {
            assistant.tick(timeProvider.getTime());
        }
    }

    private void onNewView(NewView msg) {
        if (msg.viewNumber > this.viewNumber) {
            this.viewNumber = msg.viewNumber;
            if (!msg.leader.equals(me)) this.iAmElected = false;
        }
    }

    private void onAbort(Abort abort) {
        abortBallot(abort.seqNo);
    }

    private void sendMissingSuccessMessages(Set<Long> missingSuccess, Member sender) {
        for (Long seqNo : missingSuccess) {
            if (successfulMessages.containsKey(seqNo)) {
                Success message = new Success(seqNo, successfulMessages.get(seqNo), successfulMsgIds.get(seqNo));
                messenger.sendTo(sender, PaxosUtils.serialize(message));
            }
        }
    }

    private void onBroadcastRequest(BroadcastRequest req) {
        if (iAmElected) {
            if (messagesCirculating.contains(req.msgId)) return;
            messagesCirculating.add(req.msgId);
            createProposal(++seqNo, req.message, req.msgId);
            assistants.add(new MultiAccept(membership, messenger, seqNo, req.message, req.msgId));
        } else {
            System.out.println("I am not the leader");
        }
    }

    private long newViewNumber() {
        int groupSize = membership.groupSize();
        long previousBallot = viewNumber/groupSize;
        viewNumber = (previousBallot+1)*groupSize + membership.getPositionInGroup();
        return viewNumber;
    }

    public void memberFailed(Member failedMember, Set<Member> aliveMembers) {
        if (me.equals(PaxosUtils.selectLeader(aliveMembers))) {
            System.out.println(me + ": taking leadership");
            assistants.add(new Election(membership, messenger, timeProvider.getTime(), newViewNumber()));
        }
    }

    private void registerViewAcceptance(ViewAccepted viewAccepted) {
        // register all proposals reported
        for (Long seqNo : viewAccepted.accepted.keySet()) {
            Acceptance acceptance = viewAccepted.accepted.get(seqNo);
            Proposal proposal = proposals.get(seqNo);
            if (proposal == null) {
                proposals.put(seqNo, new Proposal(acceptance.viewNumber, acceptance.message, acceptance.msgId));
            } else {
                proposal.acceptOutcome(acceptance.viewNumber, acceptance.message, acceptance.msgId);
            }
        }
    }

    private void createProposal(long seqNo, Serializable message, long msgId) {
        proposals.put(seqNo, new Proposal(viewNumber, message, msgId));
    }

    private void registerAcceptance(long viewNo, long seqNo, long msgId) {
        proposals.get(seqNo).acceptDefault(viewNo, msgId);
    }

    private void abortBallot(long seqNo) {
        proposals.remove(seqNo);
    }

    private class Election extends MultiRequest<NewView, ViewAccepted> {
        private final long viewNumber;

        public Election(GroupMembership membership, CommLayer messenger, long time, long viewNumber) {
            super(membership, messenger, new NewView(me, viewNumber), time);
            this.viewNumber = viewNumber;
        }

        @Override
        protected ViewAccepted filterResponse(Serializable message) {
            if (message instanceof ViewAccepted) {
                ViewAccepted viewAccepted = (ViewAccepted) message;
                if (viewAccepted.viewNumber != viewNumber) return null;
                registerViewAcceptance(viewAccepted);
                return viewAccepted;
            } else {
                return null;
            }
        }

        @Override
        protected void onQuorumReached() {
            System.out.println(me + ": I am the leader");
            iAmElected = true;

            // send accept for all seqNo where we have a proposal
            for (Long seqNo : proposals.keySet()) {
                Proposal proposal = proposals.get(seqNo);
                if (proposal != null) {
                    Serializable choice = proposal.newestOutcome;
                    long msgId = proposal.getMsgId();
                    messagesCirculating.add(msgId);
                    assistants.add(new MultiAccept(membership, messenger, seqNo, choice, msgId));
                }
            }

            // send NoOp for all the gaps
            LeaderLogic.this.seqNo = PaxosUtils.findMax(proposals.keySet());
            for (long seqNo = 1; seqNo < LeaderLogic.this.seqNo; seqNo++) {
                if (!proposals.containsKey(seqNo)) {
                    createProposal(seqNo, NO_OP, 0l);
                    assistants.add(new MultiAccept(membership, messenger, seqNo, NO_OP, 0l));
                }
            }
        }
    }

    private class MultiAccept extends MultiRequest<Accept, Accepted> {
        private final long seqNo;
        private final Serializable message;
        private final long msgId;

        public MultiAccept(GroupMembership membership, CommLayer messenger, long seqNo, Serializable message, long msgId) {
            super(membership, messenger, new Accept(viewNumber, seqNo, message, msgId, me), timeProvider.getTime());
            this.seqNo = seqNo;
            this.message = message;
            this.msgId = msgId;
        }

        @Override
        protected Accepted filterResponse(Serializable message) {
            if (message instanceof Accepted) {
                Accepted accepted = (Accepted) message;
                if (accepted.viewNo != viewNumber || accepted.seqNo != seqNo) return null;
                registerAcceptance(accepted.viewNo, accepted.seqNo, accepted.msgId);
                sendMissingSuccessMessages(accepted.missingSuccess, accepted.sender);
                return accepted;
            } else return null;
        }

        @Override
        protected void onQuorumReached() {
            successfulMessages.put(seqNo, message);
            successfulMsgIds.put(seqNo, msgId);
            assistants.add(new MultiSuccess(membership, messenger, seqNo, message, msgId));
        }
    }

    private class MultiSuccess extends MultiRequest<Success, SuccessAck> {
        private final long seqNo;
        private final long msgId;

        public MultiSuccess(GroupMembership membership, CommLayer messenger, long seqNo, Serializable msg, long msgId) {
            super(membership, messenger, new Success(seqNo, msg, msgId), timeProvider.getTime());
            this.seqNo = seqNo;
            this.msgId = msgId;
        }

        @Override
        protected SuccessAck filterResponse(Serializable message) {
            if (message instanceof SuccessAck) {
                SuccessAck successAck = (SuccessAck) message;
                return (msgId != successAck.getMsgId()) ? null : successAck;
            } else return null;
        }

        @Override
        protected void onCompleted() {
            // we can forget about the message now
            successfulMessages.remove(seqNo);
            successfulMsgIds.remove(msgId);
            messagesCirculating.remove(msgId);
            finish();
        }
    }
}
