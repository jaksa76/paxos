package paxos;

import org.junit.Test;
import paxos.messages.MessageWithSender;

import java.io.Serializable;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.validateMockitoUsage;
import static org.mockito.Mockito.verify;
import static paxos.TestUtils.verifyNoMoreCommunication;

public class MultiRequestTest {
    @Test
    public void testSendingAndReplying() throws Exception {
        List<Member> members = TestUtils.createMembersOnLocalhost(3);
        Messenger messenger = TestUtils.createMock(members.get(0), members);
        final boolean[] quorumReached = new boolean[] {false};
        final boolean[] completed = new boolean[] {false};

        MultiRequest<String, DummyResponse> multiRequest = new MultiRequest<String, DummyResponse>(messenger, "hello") {
            @Override protected void onQuorumReached() {
                quorumReached[0] = true;
            }

            @Override protected void onCompleted() {
                completed[0] = true;
            }
        };

        verify(messenger).sendToAll("hello");

        multiRequest.receive(new DummyResponse("yes", members.get(0)));
        assertFalse(quorumReached[0]);
        assertFalse(completed[0]);
        assertFalse(multiRequest.isFinished());

        multiRequest.receive(new DummyResponse("yes", members.get(1)));
        assertTrue(quorumReached[0]);
        assertFalse(completed[0]);
        assertFalse(multiRequest.isFinished());

        multiRequest.receive(new DummyResponse("yes", members.get(2)));
        assertTrue(quorumReached[0]);
        assertTrue(completed[0]);
        assertFalse(multiRequest.isFinished());

        verifyNoMoreCommunication(messenger);
    }

    @Test
    public void testFilteringWrongMessages() throws Exception {
        List<Member> members = TestUtils.createMembersOnLocalhost(3);
        Messenger messenger = TestUtils.createMock(members.get(0), members);
        final boolean[] quorumReached = new boolean[] {false};
        final boolean[] completed = new boolean[] {false};

        MultiRequest<String, DummyResponse> multiRequest = new MultiRequest<String, DummyResponse>(messenger, "hello", 0l) {
            @Override
            protected DummyResponse filterResponse(Serializable message) {
                return (message instanceof DummyResponse) ? (DummyResponse) message : null;
            }

            @Override protected void onQuorumReached() {
                quorumReached[0] = true;
            }

            @Override protected void onCompleted() {
                completed[0] = true;
            }
        };

        multiRequest.receive(new WrongResponse("yes", members.get(0)));
        multiRequest.receive(new WrongResponse("yes", members.get(1)));
        multiRequest.receive(new WrongResponse("yes", members.get(2)));

        assertFalse(quorumReached[0]);
        assertFalse(completed[0]);
        assertFalse(multiRequest.isFinished());
    }

    @Test
    public void testResending() throws Exception {
        List<Member> members = TestUtils.createMembersOnLocalhost(3);
        Messenger messenger = TestUtils.createMock(members.get(0), members);
        final boolean[] quorumReached = new boolean[] {false};
        final boolean[] completed = new boolean[] {false};

        MultiRequest<String, DummyResponse> multiRequest = new MultiRequest<String, DummyResponse>(messenger, "hello", 0l) {
            @Override protected void onQuorumReached() {
                quorumReached[0] = true;
            }

            @Override protected void onCompleted() {
                completed[0] = true;
            }
        };

        verify(messenger).sendToAll("hello");

        multiRequest.receive(new DummyResponse("yes", members.get(0)));
        assertFalse(quorumReached[0]);
        assertFalse(completed[0]);
        assertFalse(multiRequest.isFinished());

        verifyNoMoreCommunication(messenger);
        multiRequest.tick(2000);
        verify(messenger).send("hello", members.get(1));
        verify(messenger).send("hello", members.get(2));

        multiRequest.receive(new DummyResponse("yes", members.get(1)));
        assertTrue(quorumReached[0]);
        assertFalse(completed[0]);
        assertFalse(multiRequest.isFinished());

        verifyNoMoreCommunication(messenger);
        multiRequest.tick(4000);
        verify(messenger, times(2)).send("hello", members.get(2));

        multiRequest.receive(new DummyResponse("yes", members.get(2)));
        assertTrue(quorumReached[0]);
        assertTrue(completed[0]);
        assertFalse(multiRequest.isFinished());

        multiRequest.tick(6000);

        verifyNoMoreCommunication(messenger);
    }

    private static class DummyResponse implements MessageWithSender {
        public String content;
        public Member sender;

        public DummyResponse(String content, Member sender) {
            this.content = content;
            this.sender = sender;
        }

        public Member getSender() { return sender; }
    }

    private static class WrongResponse implements MessageWithSender {
        public String content;
        public Member sender;

        public WrongResponse(String content, Member sender) {
            this.content = content;
            this.sender = sender;
        }

        public Member getSender() { return sender; }
    }

}