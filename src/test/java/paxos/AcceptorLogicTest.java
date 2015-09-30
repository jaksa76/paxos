package paxos;

import org.junit.Test;
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
        Messenger messenger = createMock(members.get(0), members);
        Receiver receiver = mock(Receiver.class);
        Member leader = members.get(1);
        long viewNo = 1;
        long seqNo = 0;
        long msgId = 1;

        AcceptorLogic acceptor = new AcceptorLogic(messenger, receiver);

        acceptor.dispatch(new NewView(leader, viewNo));
        verify(messenger).send(specialMessage(VIEW_ACCEPTED), eq(leader));

        String message = "hello";
        startBroadcast(acceptor, message);
        verify(messenger).send(specialMessage(BROADCAST_REQ), eq(leader));

        acceptor.dispatch(new Accept(viewNo, seqNo, message, msgId, leader));
        verify(messenger).send(specialMessage(ACCEPTED), eq(leader));

        acceptor.dispatch(new Success(seqNo, "hello", msgId));
        verify(messenger).send(specialMessage(SUCCESS_ACK), eq(leader));
        verify(receiver).receive("hello");

        verifyNoMoreCommunication(messenger);
        verifyNoMoreInteractions(receiver);
    }

    @Test
    public void testConsecutiveMessages() throws Exception {
        List<Member> members = asList(localMember(2440), localMember(2441));
        Messenger messenger = createMock(members.get(0), members);
        Receiver receiver = mock(Receiver.class);
        Member leader = members.get(1);
        long viewNo = 1;
        long msgId1 = 1, msgId2 = 2;

        AcceptorLogic acceptor = new AcceptorLogic(messenger, receiver);

        acceptor.dispatch(new NewView(leader, viewNo));
        verify(messenger).send(specialMessage(VIEW_ACCEPTED), eq(leader));

        acceptor.dispatch(new Accept(viewNo, 0, "hello", msgId1, leader));
        verify(messenger).send(specialMessage(ACCEPTED), eq(leader));

        acceptor.dispatch(new Success(0, "hello", msgId1));
        verify(messenger).send(specialMessage(SUCCESS_ACK), eq(leader));
        verify(receiver).receive("hello");

        acceptor.dispatch(new Accept(viewNo, 1, "good morning", msgId2, leader));
        verify(messenger, times(2)).send(specialMessage(ACCEPTED), eq(leader));

        acceptor.dispatch(new Success(1, "good morning", msgId2));
        verify(messenger, times(2)).send(specialMessage(SUCCESS_ACK), eq(leader));
        verify(receiver).receive("good morning");

        verifyNoMoreCommunication(messenger);
        verifyNoMoreInteractions(receiver);
    }

    @Test
    public void testTakingOver() throws Exception {
        String message1 = "hello";
        String message2 = "hi";
        List<Member> members = TestUtils.createMembersOnLocalhost(3);
        Messenger messenger = createMock(members.get(0), members);
        Receiver receiver = mock(Receiver.class);
        Member leader = members.get(1);
        long view1 = 1, view2 = 2;
        long seqNo = 0;
        long msgId1 = 1; long msgId2 = 2;


        AcceptorLogic acceptor = new AcceptorLogic(messenger, receiver);

        acceptor.dispatch(new NewView(leader, view1));
        verify(messenger).send(specialMessage(VIEW_ACCEPTED), eq(leader));

        acceptor.dispatch(new Accept(view1, seqNo, message1, msgId1, leader));
        verify(messenger).send(specialMessage(ACCEPTED), eq(leader));

        leader = members.get(2);
        acceptor.dispatch(new NewView(leader, view2));
        verify(messenger).send(specialMessage(VIEW_ACCEPTED), eq(leader));

        acceptor.dispatch(new Accept(view2, seqNo, message2, msgId2, leader));
        verify(messenger).send(specialMessage(ACCEPTED), eq(leader));

        acceptor.dispatch(new Success(seqNo, message2, msgId2));
        verify(messenger).send(specialMessage(SUCCESS_ACK), eq(leader));
        verify(receiver).receive(message2);

        verifyNoMoreCommunication(messenger);
        verifyNoMoreInteractions(receiver);
    }

    @Test
    public void testCatchingUpMissingSuccessMessages() throws Exception {
        List<Member> members = asList(localMember(2440), localMember(2441));
        Messenger messenger = createMock(members.get(0), members);
        Receiver receiver = mock(Receiver.class);
        Member leader = members.get(1);
        long viewNo = 1;
        long msgId1 = 1, msgId2 = 2;

        AcceptorLogic acceptor = new AcceptorLogic(messenger, receiver);

        acceptor.dispatch(new NewView(leader, viewNo));
        verify(messenger).send(specialMessage(VIEW_ACCEPTED), eq(leader));

        acceptor.dispatch(new Accept(viewNo, 0, "hello", msgId1, leader));
        verify(messenger).send(specialMessage(ACCEPTED), eq(leader));

        acceptor.dispatch(new Accept(viewNo, 1, "good morning", msgId2, leader));
        verify(messenger).send(acceptedMessageWithMissingList(0l), eq(leader));

        acceptor.dispatch(new Success(0l, "hello", msgId1));
        acceptor.dispatch(new Success(1l, "good morning", msgId2));
        verify(messenger, times(2)).send(specialMessage(SUCCESS_ACK), eq(leader));
        verify(receiver).receive("hello");
        verify(receiver).receive("good morning");

        verifyNoMoreCommunication(messenger);
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
