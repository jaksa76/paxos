package paxos;

import org.junit.Test;
import paxos.communication.CommLayer;
import paxos.communication.Member;
import paxos.communication.Tick;
import paxos.messages.*;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.*;

import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.EMPTY_SET;
import static java.util.Collections.sort;
import static org.mockito.Mockito.*;
import static paxos.TestUtils.*;
import static paxos.messages.SpecialMessage.MessageType.*;

@SuppressWarnings("ALL")
public class LeaderLogicTest {

    public static final Serializable NO_OP = new NoOp();
    private LeaderLogic leader;
    private CommLayer messenger;
    private List<Member> members;
    private GroupMembership membership;
    private TestTimeProvider timeProvider = new TestTimeProvider();
    private long viewNo;

    @Test
    public void testSimplePath() throws Exception {
        members = TestUtils.createMembersOnLocalhost(2);
        membership = createMembership(members, 1);
        messenger = mock(CommLayer.class);
        long viewNo = 3l;
        long seqNo = 0l;
        long msgId = 1l;

        leader = new LeaderLogic(membership, messenger);
        verify(messenger).sendTo(eq(members), specialMessage(NEW_VIEW));

        leader.dispatch(new ViewAccepted(viewNo, EMPTY_MAP, members.get(0)));
        leader.dispatch(new ViewAccepted(viewNo, EMPTY_MAP, members.get(1)));

        leader.dispatch(new BroadcastRequest("hello", msgId));
        verify(messenger).sendTo(eq(members), specialMessage(ACCEPT));

        leader.dispatch(new Accepted(viewNo, seqNo, msgId, EMPTY_SET, members.get(0)));

        leader.dispatch(new Accepted(viewNo, seqNo, msgId, EMPTY_SET, members.get(1)));
        verify(messenger).sendTo(eq(members), specialMessage(SUCCESS));

        verifyNoMoreInteractions((CommLayer) messenger);
    }

    @Test
    public void testNotTakingLeadershipIfNotHighest() throws Exception {
        members = TestUtils.createMembersOnLocalhost(2);
        membership = createMembership(members, 0);
        messenger = mock(CommLayer.class);

        leader = new LeaderLogic(membership, messenger);
        verifyNoMoreInteractions((CommLayer) messenger);
    }

    @Test
    public void testRepeatingBroadcastReqDoesNotRepeatBroadcast() throws Exception {
        createGroup(2);

        leader.dispatch(new BroadcastRequest("hello", 1));
        verify(messenger).sendTo(eq(members), specialMessage(ACCEPT));

        leader.dispatch(new BroadcastRequest("hello", 1));

        verifyNoMoreInteractions((CommLayer) messenger);
    }

    @Test
    public void testBroadcastIsRepeatedAfterTimeout() throws Exception {
        createGroup(2);

        long msgId = 1l;
        leader.dispatch(new BroadcastRequest("hello", msgId));
        verify(messenger).sendTo(eq(members), specialMessage(ACCEPT));

        advanceTimeTo(1500);
        for (Member member : members) {
            verify(messenger).sendTo(eq(member), specialMessage(ACCEPT));
        }

        verifyNoMoreInteractions((CommLayer) messenger);
    }

    @Test
    public void testConsecutiveMessages() throws Exception {
        createGroup(2);

        long viewNo = 3;
        long msgId1 = 1, msgId2 = 2;

        leader.dispatch(new BroadcastRequest("hello", msgId1));
        verify(messenger).sendTo(eq(members), specialMessage(ACCEPT));

        leader.dispatch(new Accepted(viewNo, 0, msgId1, EMPTY_SET, members.get(0)));
        leader.dispatch(new Accepted(viewNo, 0, msgId1, EMPTY_SET, members.get(1)));
        verify(messenger).sendTo(eq(members), specialMessage(SUCCESS));

        leader.dispatch(new BroadcastRequest("good morning", msgId2));
        verify(messenger, times(2)).sendTo(eq(members), specialMessage(ACCEPT));

        leader.dispatch(new Accepted(viewNo, 1, msgId2, EMPTY_SET, members.get(0)));
        leader.dispatch(new Accepted(viewNo, 1, msgId2, EMPTY_SET, members.get(1)));
        verify(messenger, times(2)).sendTo(eq(members), specialMessage(SUCCESS));

        verifyNoMoreInteractions((CommLayer) messenger);
    }

    @Test
    public void testTakingOver() throws Exception {
        members = TestUtils.createMembersOnLocalhost(3);
        membership = createMembership(members, 1);
        CommLayer messenger2 = mock(CommLayer.class);
        long newViewNo = 4;
        long msgId1 = 1l, msgId2 = 20;

        Map<Long,Acceptance> previousMessages = new HashMap<Long, Acceptance>();
        previousMessages.put(2l, new Acceptance(1, "a", msgId1));
        previousMessages.put(4l, new Acceptance(1, "b", msgId2));

        LeaderLogic newLeader = new LeaderLogic(membership, messenger2);

        newLeader.memberFailed(members.get(2), new HashSet<Member>(Arrays.asList(members.get(0), members.get(1))));
        verify(messenger2).sendTo(eq(members), specialMessage(NEW_VIEW));

        newLeader.dispatch(new ViewAccepted(newViewNo, EMPTY_MAP, members.get(0)));
        newLeader.dispatch(new ViewAccepted(newViewNo, previousMessages, members.get(1)));
        verify(messenger2).sendTo(eq(members), acceptMessage(1, NO_OP));
        verify(messenger2).sendTo(eq(members), acceptMessage(2, "a"));
        verify(messenger2).sendTo(eq(members), acceptMessage(3, NO_OP));
        verify(messenger2).sendTo(eq(members), acceptMessage(4, "b"));

        newLeader.dispatch(new Accepted(newViewNo, 1, 0, EMPTY_SET, members.get(0)));
        newLeader.dispatch(new Accepted(newViewNo, 2, msgId1, EMPTY_SET, members.get(0)));
        newLeader.dispatch(new Accepted(newViewNo, 3, 0, EMPTY_SET, members.get(0)));
        newLeader.dispatch(new Accepted(newViewNo, 4, msgId2, EMPTY_SET, members.get(0)));

        newLeader.dispatch(new Accepted(newViewNo, 1, 0, EMPTY_SET, members.get(1)));
        newLeader.dispatch(new Accepted(newViewNo, 2, msgId1, EMPTY_SET, members.get(1)));
        newLeader.dispatch(new Accepted(newViewNo, 3, 0, EMPTY_SET, members.get(1)));
        newLeader.dispatch(new Accepted(newViewNo, 4, msgId2, EMPTY_SET, members.get(1)));
        verify(messenger2, times(4)).sendTo(eq(members), specialMessage(SUCCESS));

        verifyNoMoreInteractions((CommLayer) messenger2);
    }

    @Test
    public void testTakingOver2() throws Exception {
        members = TestUtils.createMembersOnLocalhost(3);
        membership = createMembership(members, 1);
        CommLayer messenger2 = mock(CommLayer.class);
        long newViewNo = 4;
        long msgId1 = 1l, msgId2 = 20;

        Map<Long,Acceptance> previousMessages = new HashMap<Long, Acceptance>();
        previousMessages.put(2l, new Acceptance(1, "a", msgId1));
        previousMessages.put(4l, new Acceptance(1, "b", msgId2));

        LeaderLogic newLeader = new LeaderLogic(membership, messenger2);

        newLeader.memberFailed(members.get(2), new HashSet<Member>(Arrays.asList(members.get(0), members.get(1))));
        verify(messenger2).sendTo(eq(members), specialMessage(NEW_VIEW));

        newLeader.dispatch(new ViewAccepted(newViewNo, EMPTY_MAP, members.get(0)));
        newLeader.dispatch(new ViewAccepted(newViewNo, previousMessages, members.get(1)));
        verify(messenger2).sendTo(eq(members), acceptMessage(1, NO_OP));
        verify(messenger2).sendTo(eq(members), acceptMessage(2, "a"));
        verify(messenger2).sendTo(eq(members), acceptMessage(3, NO_OP));
        verify(messenger2).sendTo(eq(members), acceptMessage(4, "b"));

        newLeader.dispatch(new BroadcastRequest("a", msgId1));

        verifyNoMoreInteractions((CommLayer) messenger2);
    }

    @Test
    public void testBeingTakenOver() throws Exception {
        members = TestUtils.createMembersOnLocalhost(3);
        membership = createMembership(members, 1);
        CommLayer messenger2 = mock(CommLayer.class);

        LeaderLogic newLeader = new LeaderLogic(membership, messenger2);

        // take over
        long newViewNo = 4;
        newLeader.memberFailed(members.get(2), asSet(members.get(0), members.get(1)));
        verify(messenger2).sendTo(eq(members), newView(newViewNo));
        newLeader.dispatch(new ViewAccepted(newViewNo, EMPTY_MAP, members.get(0)));
        newLeader.dispatch(new ViewAccepted(newViewNo, EMPTY_MAP, members.get(1)));

        // old leader takes back leadership
        newLeader.dispatch(new NewView(members.get(2), 5));

        newLeader.dispatch(new BroadcastRequest("hello", 10)); // this should NOT trigger an accept message

        verifyNoMoreInteractions((CommLayer) messenger2);
    }

    @Test
    public void testTakingOverWithMorePredecessors() throws Exception {
        members = TestUtils.createMembersOnLocalhost(4);
        membership = createMembership(members, 2);
        CommLayer messenger = mock(CommLayer.class);
        long newViewNo = 6;
        long msgId1 = 10, msgId2 = 20;

        Map<Long,Acceptance> previousMessagesFromA = new HashMap<Long, Acceptance>();
        previousMessagesFromA.put(2l, new Acceptance(1, "a", msgId1));

        Map<Long,Acceptance> previousMessagesFromB = new HashMap<Long, Acceptance>();
        previousMessagesFromB.put(2l, new Acceptance(2, "b", msgId2));

        Member oldLeader = members.get(3);
        LeaderLogic newLeader = new LeaderLogic(membership, messenger);
        newLeader.dispatch(new NewView(oldLeader, 2l));

        HashSet<Member> aliveMembers = new HashSet<Member>(members);
        aliveMembers.remove(oldLeader);
        newLeader.memberFailed(oldLeader, aliveMembers);
        verify(messenger).sendTo(eq(members), newView(newViewNo));

        newLeader.dispatch(new ViewAccepted(newViewNo, previousMessagesFromA, members.get(0)));
        newLeader.dispatch(new ViewAccepted(newViewNo, previousMessagesFromB, members.get(1)));
        newLeader.dispatch(new ViewAccepted(newViewNo, EMPTY_MAP, members.get(2)));
        verify(messenger).sendTo(eq(members), acceptMessage(2, "b"));
        verify(messenger).sendTo(eq(members), acceptMessage(1, NO_OP));

        verifyNoMoreInteractions((CommLayer) messenger);
    }

    @Test
    public void testCatchingUpMissingSuccessMessages() throws Exception {
        createGroup(2);
        long msgId1 = 10, msgId2 = 20;

        // broadcast message 1
        leader.dispatch(new BroadcastRequest("hello", msgId1));
        verify(messenger).sendTo(eq(members), specialMessage(ACCEPT));
        mockAcceptedFromAllMembers(0, msgId1);
        verify(messenger).sendTo(eq(members), specialMessage(SUCCESS));

        // broadcast message 2
        leader.dispatch(new BroadcastRequest("good morning", msgId2));
        verify(messenger, times(2)).sendTo(eq(members), specialMessage(ACCEPT));

        leader.dispatch(new Accepted(viewNo, 1, msgId2, TestUtils.asSet(0l), members.get(0)));
        leader.dispatch(new Accepted(viewNo, 1, msgId2, EMPTY_SET, members.get(1)));
        verify(messenger).sendTo(eq(members.get(0)), specialMessage(SUCCESS));

        // now both broadcasts have completed
        verify(messenger, times(2)).sendTo(eq(members), specialMessage(SUCCESS));

        verifyNoMoreInteractions((CommLayer) messenger);
    }

    @Test
    public void testFilteringSuccessAckMessages() throws Exception {
        createGroup(3);

        leader.dispatch(new BroadcastRequest("hello", 1));
        verify(messenger).sendTo(eq(members), specialMessage(ACCEPT));

        mockAcceptedFromAllMembers(0, 1);
        verify(messenger).sendTo(eq(members), specialMessage(SUCCESS));

        leader.dispatch(new TestMessageWithSender(members.get(0)));
        leader.dispatch(new TestMessageWithSender(members.get(1)));

        leader.dispatch(new BroadcastRequest("hello", 1));

        verifyNoMoreInteractions((CommLayer) messenger);
    }

    @Test
    public void testCompetingLeaders() throws Exception {
        createGroup(3);

        // a different member announces leadership
        leader.dispatch(new NewView(members.get(1), ++viewNo));

        leader.dispatch(new BroadcastRequest("a", 10)); // this message should be ignored

        leader.memberFailed(members.get(1), asSet(members.get(0), members.get(2)));
        verify(messenger, times(2)).sendTo(eq(members), specialMessage(NEW_VIEW));

        verifyNoMoreInteractions((CommLayer) messenger);
    }

    private void mockAcceptedFromAllMembers(long seqNo, long msgId) {
        for (Member member : members) {
            leader.dispatch(new Accepted(viewNo, seqNo, msgId, EMPTY_SET, member));
        }
    }

    private void createGroup(int size) throws UnknownHostException {
        members = createMembersOnLocalhost(size);
        membership = createMembership(members, size-1);
        messenger = mock(CommLayer.class);
        viewNo = (size*2)-1;

        leader = new LeaderLogic(membership, messenger, timeProvider);
        performSuccessfulElection(viewNo);
    }

    private void performSuccessfulElection(long viewNo) {
        verify(messenger).sendTo(eq(members), newView(viewNo));
        for (Member member : members) {
            leader.dispatch(new ViewAccepted(viewNo, EMPTY_MAP, member));
        }
    }

    private void advanceTimeTo(int time) {
        leader.update(new Tick(time));
    }

    private class TestMessageWithSender implements MessageWithSender {
        private final Member sender;

        TestMessageWithSender(Member sender) {
            this.sender = sender;
        }

        public Member getSender() {
            return sender;
        }
    }
}
