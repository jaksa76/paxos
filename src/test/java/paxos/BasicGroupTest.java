package paxos;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import paxos.communication.CommLayer;
import paxos.communication.Member;
import paxos.messages.*;

import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.*;
import static paxos.TestUtils.message;

public class BasicGroupTest {
    CommLayer commLayer = mock(CommLayer.class);
    Receiver receiver = mock(Receiver.class);

    @Test
    public void testSupposedNonLeaderDoesntStartElection() throws Exception {
        List<Member> members = TestUtils.createMembersOnLocalhost(3);
        GroupMembership membership = TestUtils.createMembership(members, 0);

        BasicGroup group = createGroup(membership);
        verifyNoMoreInteractions(commLayer);
    }

    @Test
    public void testSupposedLeaderStartsElection() throws Exception {
        List<Member> members = TestUtils.createMembersOnLocalhost(3);
        GroupMembership membership = TestUtils.createMembership(members, 2);

        BasicGroup group = createGroup(membership);

        verify(commLayer).sendTo(eq(members), message(instanceOf(NewView.class)));
        verifyNoMoreInteractions(commLayer);
    }

    @Test
    public void testSupposedNonLeaderStartsElectionWhenLeaderSeemsDead() throws Exception {
        List<Member> members = TestUtils.createMembersOnLocalhost(3);
        GroupMembership membership = TestUtils.createMembership(members, 1);

        BasicGroup group = createGroup(membership);
        verifyNoMoreInteractions(commLayer);

        group.receive(TestUtils.tick(5000)); // this should be enough to trigger the failure detection
        verify(commLayer).sendTo(eq(members), message(instanceOf(NewView.class)));
    }

    @Test
    public void testMemberChangesLeaderOnTakeOver() throws Exception {
        List<Member> members = TestUtils.createMembersOnLocalhost(3);
        GroupMembership membership = TestUtils.createMembership(members, 0);

        BasicGroup group = createGroup(membership);
        verifyNoMoreInteractions(commLayer);

        group.receive(PaxosUtils.serialize(new NewView(members.get(1), 2)));
        verify(commLayer).sendTo(eq(members.get(1)), message(instanceOf(ViewAccepted.class)));
    }

    @Test
    public void testMemberDoesntChangeLeaderIfTooLate() throws Exception {
        List<Member> members = TestUtils.createMembersOnLocalhost(3);
        GroupMembership membership = TestUtils.createMembership(members, 0);

        BasicGroup group = createGroup(membership);
        verifyNoMoreInteractions(commLayer);

        group.receive(PaxosUtils.serialize(new NewView(members.get(1), 2)));
        verify(commLayer).sendTo(eq(members.get(1)), message(instanceOf(ViewAccepted.class)));

        group.receive(PaxosUtils.serialize(new NewView(members.get(2), 1)));
        verifyNoMoreInteractions(commLayer);
    }

    @Test
    public void testMemberAcceptsMessageFromLeader() throws Exception {
        List<Member> members = TestUtils.createMembersOnLocalhost(3);
        GroupMembership membership = TestUtils.createMembership(members, 0);

        BasicGroup group = createGroup(membership);
        verifyNoMoreInteractions(commLayer);

        group.receive(PaxosUtils.serialize(new NewView(members.get(2), 1)));
        verify(commLayer).sendTo(eq(members.get(2)), message(instanceOf(ViewAccepted.class)));

        group.receive(PaxosUtils.serialize(new Accept(1, 1, "Hello", 1, members.get(2))));
        verify(commLayer).sendTo(eq(members.get(2)), message(instanceOf(Accepted.class)));
    }

    @Test
    public void testMemberAcceptsMessageFromNewLeader() throws Exception {
        List<Member> members = TestUtils.createMembersOnLocalhost(3);
        GroupMembership membership = TestUtils.createMembership(members, 0);

        BasicGroup group = createGroup(membership);
        verifyNoMoreInteractions(commLayer);

        group.receive(PaxosUtils.serialize(new NewView(members.get(2), 1)));
        verify(commLayer).sendTo(eq(members.get(2)), message(instanceOf(ViewAccepted.class)));

        group.receive(PaxosUtils.serialize(new NewView(members.get(1), 2)));
        verify(commLayer).sendTo(eq(members.get(1)), message(instanceOf(ViewAccepted.class)));

        group.receive(PaxosUtils.serialize(new Accept(2, 1, "Hello", 1, members.get(1))));
        verify(commLayer).sendTo(eq(members.get(1)), message(instanceOf(Accepted.class)));
    }

    @Test
    public void testMemberDoesntAcceptMessageFromOldLeader() throws Exception {
        List<Member> members = TestUtils.createMembersOnLocalhost(3);
        GroupMembership membership = TestUtils.createMembership(members, 0);

        BasicGroup group = createGroup(membership);
        verifyNoMoreInteractions(commLayer);

        Member defaultLeader = members.get(2);
        group.receive(PaxosUtils.serialize(new NewView(defaultLeader, 1)));
        verify(commLayer).sendTo(eq(defaultLeader), message(instanceOf(ViewAccepted.class)));

        Member newLeader = members.get(1);
        group.receive(PaxosUtils.serialize(new NewView(newLeader, 2)));
        verify(commLayer).sendTo(eq(newLeader), message(instanceOf(ViewAccepted.class)));

        group.receive(PaxosUtils.serialize(new Accept(1, 1, "Hello", 1, defaultLeader)));
        verify(commLayer).sendTo(eq(defaultLeader), message(instanceOf(Abort.class)));
    }

    @Test
    public void testMemberNotifiesNewLeaderOfMessages() throws Exception {
        List<Member> members = TestUtils.createMembersOnLocalhost(3);
        GroupMembership membership = TestUtils.createMembership(members, 0);

        BasicGroup group = createGroup(membership);
        verifyNoMoreInteractions(commLayer);

        Member defaultLeader = members.get(2);
        group.receive(PaxosUtils.serialize(new NewView(defaultLeader, 1)));
        verify(commLayer).sendTo(eq(defaultLeader), message(instanceOf(ViewAccepted.class)));

        group.receive(PaxosUtils.serialize(new Accept(1, 1, "Hello", 1, defaultLeader)));
        verify(commLayer).sendTo(eq(defaultLeader), message(instanceOf(Accepted.class)));

        Member newLeader = members.get(1);
        group.receive(PaxosUtils.serialize(new NewView(newLeader, 2)));
        verify(commLayer).sendTo(eq(newLeader), message(viewAccepted(1)));
    }

    private Matcher viewAccepted(final long id) {
        return new TypeSafeMatcher<ViewAccepted>() {
            @Override
            protected boolean matchesSafely(ViewAccepted viewAccepted) {
                return viewAccepted.accepted.containsKey(id);
            }

            public void describeTo(Description description) {
                description.appendText("a ViewAccepted message referecing msg id " + id);
            }
        };
    }

    private BasicGroup createGroup(GroupMembership membership) {
        BasicGroup group = new BasicGroup(membership, commLayer, receiver);
        verify(commLayer).setListener((CommLayer.MessageListener) any());
        group.receive(TestUtils.tick(0)); // initializes the time
        return group;
    }
}
