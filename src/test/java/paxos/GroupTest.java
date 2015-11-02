package paxos;

import org.junit.Test;
import paxos.communication.CommLayer;
import paxos.communication.Member;
import paxos.messages.NewView;

import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.*;
import static paxos.TestUtils.message;

public class GroupTest {
    CommLayer commLayer = mock(CommLayer.class);
    Receiver receiver = mock(Receiver.class);

    @Test
    public void testSupposedNonLeaderDoesntStartElection() throws Exception {
        List<Member> members = TestUtils.createMembersOnLocalhost(3);
        GroupMembership membership = TestUtils.createMembership(members, 0);

        Group group = createGroup(membership);
        verifyNoMoreInteractions(commLayer);
    }

    @Test
    public void testSupposedLeaderStartsElection() throws Exception {
        List<Member> members = TestUtils.createMembersOnLocalhost(3);
        GroupMembership membership = TestUtils.createMembership(members, 2);

        Group group = createGroup(membership);

        verify(commLayer).sendTo(eq(members), message(instanceOf(NewView.class)));
        verifyNoMoreInteractions(commLayer);
    }

    @Test
    public void testSupposedNonLeaderStartsElectionWhenLeaderSeemsDead() throws Exception {
        List<Member> members = TestUtils.createMembersOnLocalhost(3);
        GroupMembership membership = TestUtils.createMembership(members, 1);

        Group group = createGroup(membership);
        verifyNoMoreInteractions(commLayer);

        group.receive(TestUtils.tick(5000));
        verify(commLayer).sendTo(eq(members), message(instanceOf(NewView.class)));
    }

    private Group createGroup(GroupMembership membership) {
        Group group = new Group(membership, commLayer, receiver);
        verify(commLayer).setListener((CommLayer.MessageListener) any());
        group.receive(TestUtils.tick(0)); // initializes the time
        return group;
    }
}
