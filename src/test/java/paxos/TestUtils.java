package paxos;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import paxos.communication.Member;
import paxos.communication.Tick;
import paxos.messages.Accept;
import paxos.messages.Accepted;
import paxos.messages.NewView;
import paxos.messages.SpecialMessage;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class TestUtils {
    public static List<Member> createMembersOnLocalhost(int n) throws UnknownHostException {
        List<Member> list = new ArrayList<Member>(n);
        for (int i = 0; i < n; i++) list.add(localMember(2440 + i));
        return list;
    }

    public static GroupMembership createMembership(List<Member> members, int n) {
        return new GroupMembership(members, members.get(n));
    }

    public static Member localMember(int port) throws UnknownHostException {
        return new Member(InetAddress.getLocalHost(), port);
    }

    public static byte[] specialMessage(final SpecialMessage.MessageType messageType) {
        return Matchers.argThat(new TypeSafeMatcher<byte[]>() {
            protected boolean matchesSafely(byte[] bytes) {
                Serializable message = (Serializable) PaxosUtils.deserialize(bytes);
                if (message instanceof SpecialMessage) {
                    SpecialMessage specialMessage = (SpecialMessage) message;
                    return specialMessage.getMessageType() == messageType;
                }
                return false;
            }

            public void describeTo(Description description) {
                description.appendText("envelope of type " + messageType.toString());
            }
        });
    }

    public static byte[] acceptMessage(final long seqNo, final Serializable msg) {
        return Matchers.argThat(new TypeSafeMatcher<byte[]>() {
            protected boolean matchesSafely(byte[] bytes) {
                Serializable message = (Serializable) PaxosUtils.deserialize(bytes);
                if (message instanceof Accept) {
                    Accept accept = (Accept) message;
                    return accept.seqNo == seqNo && msg.equals(accept.message);
                }
                return false;
            }

            public void describeTo(Description description) {
                description.appendText("An ACCEPT message with seqNo: " + seqNo + " message: " + msg);
            }
        });
    }

    public static byte[] newView(final long viewNo) {
        return Matchers.argThat(deserialized(new TypeSafeMatcher<NewView>() {
            protected boolean matchesSafely(NewView newView) {
                return newView.viewNumber == viewNo;
            }

            public void describeTo(Description description) {
                description.appendText("A NEW_VIEW message with viewNo: " + viewNo);
            }
        }));
    }

    public static byte[] message(Matcher m) {
        return Matchers.argThat(deserialized(m));
    }

    public static Matcher<byte[]> deserialized(final Matcher m) {
        return new TypeSafeMatcher<byte[]>() {
            protected boolean matchesSafely(byte[] bytes) {
                return m.matches(PaxosUtils.deserialize(bytes));
            }

            public void describeTo(Description description) {
                m.describeTo(description);
            }
        };
    }

    public static byte[] acceptedMessageWithMissingList(final long... seqNos) {
        return Matchers.argThat(new TypeSafeMatcher<byte[]>() {
            protected boolean matchesSafely(byte[] bytes) {
                Serializable message = (Serializable) PaxosUtils.deserialize(bytes);
                if (message instanceof Accepted) {
                    Accepted accepted = (Accepted) message;
                    if (seqNos.length != accepted.missingSuccess.size()) return false;
                    for (long seqNo : seqNos) {
                        if (!accepted.missingSuccess.contains(seqNo)) return false;
                    }
                    return true;
                }
                return false;
            }

            public void describeTo(Description description) {
                description.appendText("ACCEPTED message with missing list : " + Arrays.toString(seqNos));
            }
        });
    }

    public static <T> Set<T> asSet(T... elements) {
        HashSet<T> set = new HashSet<T>();
        Collections.addAll(set, elements);
        return set;
    }

    public static void waitTillFinished(Thread[] threads) throws InterruptedException {
        for (Thread t : threads) t.join();
        Thread.sleep(300);
    }

    public static byte[] tick(long time) {
        return PaxosUtils.serialize(new Tick(time));
    }

    public static class ArgCaptor<T> implements Answer {
        public T arg;
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
            arg = (T) invocationOnMock.getArguments()[0];
            return null;
        }
    }
}
