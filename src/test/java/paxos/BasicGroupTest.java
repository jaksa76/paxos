package paxos;

import org.junit.Test;
import paxos.communication.CommLayer;
import paxos.communication.Member;
import paxos.messages.NewView;

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

    private BasicGroup createGroup(GroupMembership membership) {
        BasicGroup group = new BasicGroup(membership, commLayer, receiver);
        verify(commLayer).setListener((CommLayer.MessageListener) any());
        group.receive(TestUtils.tick(0)); // initializes the time
        return group;
    }
}
