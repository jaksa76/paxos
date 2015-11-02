package paxos;

import org.junit.Test;
import paxos.communication.CommLayer;
import paxos.communication.Member;
import paxos.messages.Accept;
import paxos.messages.NewView;
import paxos.messages.Success;

import java.io.Serializable;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Arrays.binarySearch;
import static org.mockito.Mockito.*;
import static paxos.TestUtils.*;
import static paxos.messages.SpecialMessage.MessageType.*;

public class AcceptorLogicTest {
    @Test
    public void testStandardPath() throws Exception {
        List<Member> members = asList(localMember(2440), localMember(2441));
        GroupMembership membership = createMembership(members, 0);
        CommLayer messenger = mock(CommLayer.class);
        Receiver receiver = mock(Receiver.class);
        Member leader = members.get(1);
        long viewNo = 1;
        long seqNo = 0;
        long msgId = 1;

        AcceptorLogic acceptor = new AcceptorLogic(membership, messenger, receiver);

        acceptor.dispatch(new NewView(leader, viewNo));
        verify(messenger).sendTo(eq(leader), specialMessage(VIEW_ACCEPTED));

        String message = "hello";
        startBroadcast(acceptor, message);
        verify(messenger).sendTo(eq(leader), specialMessage(BROADCAST_REQ));

        acceptor.dispatch(new Accept(viewNo, seqNo, message, msgId, leader));
        verify(messenger).sendTo(eq(leader), specialMessage(ACCEPTED));

        acceptor.dispatch(new Success(seqNo, "hello", msgId));
        verify(messenger).sendTo(eq(leader), specialMessage(SUCCESS_ACK));
        verify(receiver).receive("hello");

        verifyNoMoreInteractions((CommLayer) messenger);
        verifyNoMoreInteractions(receiver);
    }

    @Test
    public void testConsecutiveMessages() throws Exception {
        List<Member> members = asList(localMember(2440), localMember(2441));
        GroupMembership membership = createMembership(members, 0);
        CommLayer messenger = mock(CommLayer.class);
        Receiver receiver = mock(Receiver.class);
        Member leader = members.get(1);
        long viewNo = 1;
        long msgId1 = 1, msgId2 = 2;

        AcceptorLogic acceptor = new AcceptorLogic(membership, messenger, receiver);

        acceptor.dispatch(new NewView(leader, viewNo));
        verify(messenger).sendTo(eq(leader), specialMessage(VIEW_ACCEPTED));

        acceptor.dispatch(new Accept(viewNo, 0, "hello", msgId1, leader));
        verify(messenger).sendTo(eq(leader), specialMessage(ACCEPTED));

        acceptor.dispatch(new Success(0, "hello", msgId1));
        verify(messenger).sendTo(eq(leader), specialMessage(SUCCESS_ACK));
        verify(receiver).receive("hello");

        acceptor.dispatch(new Accept(viewNo, 1, "good morning", msgId2, leader));
        verify(messenger, times(2)).sendTo(eq(leader), specialMessage(ACCEPTED));

        acceptor.dispatch(new Success(1, "good morning", msgId2));
        verify(messenger, times(2)).sendTo(eq(leader), specialMessage(SUCCESS_ACK));
        verify(receiver).receive("good morning");

        verifyNoMoreInteractions((CommLayer) messenger);
        verifyNoMoreInteractions(receiver);
    }

    @Test
    public void testTakingOver() throws Exception {
        String message1 = "hello";
        String message2 = "hi";
        List<Member> members = TestUtils.createMembersOnLocalhost(3);
        GroupMembership membership = createMembership(members, 0);
        CommLayer messenger = mock(CommLayer.class);
        Receiver receiver = mock(Receiver.class);
        Member leader = members.get(1);
        long view1 = 1, view2 = 2;
        long seqNo = 0;
        long msgId1 = 1; long msgId2 = 2;


        AcceptorLogic acceptor = new AcceptorLogic(membership, messenger, receiver);

        acceptor.dispatch(new NewView(leader, view1));
        verify(messenger).sendTo(eq(leader), specialMessage(VIEW_ACCEPTED));

        acceptor.dispatch(new Accept(view1, seqNo, message1, msgId1, leader));
        verify(messenger).sendTo(eq(leader), specialMessage(ACCEPTED));

        leader = members.get(2);
        acceptor.dispatch(new NewView(leader, view2));
        verify(messenger).sendTo(eq(leader), specialMessage(VIEW_ACCEPTED));

        acceptor.dispatch(new Accept(view2, seqNo, message2, msgId2, leader));
        verify(messenger).sendTo(eq(leader), specialMessage(ACCEPTED));

        acceptor.dispatch(new Success(seqNo, message2, msgId2));
        verify(messenger).sendTo(eq(leader), specialMessage(SUCCESS_ACK));
        verify(receiver).receive(message2);

        verifyNoMoreInteractions((CommLayer) messenger);
        verifyNoMoreInteractions(receiver);
    }

    @Test
    public void testCatchingUpMissingSuccessMessages() throws Exception {
        List<Member> members = asList(localMember(2440), localMember(2441));
        GroupMembership membership = createMembership(members, 0);
        CommLayer messenger = mock(CommLayer.class);
        Receiver receiver = mock(Receiver.class);
        Member leader = members.get(1);
        long viewNo = 1;
        long msgId1 = 1, msgId2 = 2;

        AcceptorLogic acceptor = new AcceptorLogic(membership, messenger, receiver);

        acceptor.dispatch(new NewView(leader, viewNo));
        verify(messenger).sendTo(eq(leader), specialMessage(VIEW_ACCEPTED));

        acceptor.dispatch(new Accept(viewNo, 0, "hello", msgId1, leader));
        verify(messenger).sendTo(eq(leader), specialMessage(ACCEPTED));

        acceptor.dispatch(new Accept(viewNo, 1, "good morning", msgId2, leader));
        verify(messenger).sendTo(eq(leader), acceptedMessageWithMissingList(0l));

        acceptor.dispatch(new Success(0l, "hello", msgId1));
        acceptor.dispatch(new Success(1l, "good morning", msgId2));
        verify(messenger, times(2)).sendTo(eq(leader), specialMessage(SUCCESS_ACK));
        verify(receiver).receive("hello");
        verify(receiver).receive("good morning");

        verifyNoMoreInteractions((CommLayer) messenger);
        verifyNoMoreInteractions(receiver);
    }

    private void startBroadcast(final AcceptorLogic acceptor, final Serializable message) throws InterruptedException {
        new Thread() {
            @Override
            public void run() {
                acceptor.broadcast(message);
            }
        }.start();
        Thread.sleep(100);
    }
}
