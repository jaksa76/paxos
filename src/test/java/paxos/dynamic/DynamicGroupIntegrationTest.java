package paxos.dynamic;

import org.junit.Test;
import org.mockito.Matchers;
import paxos.*;

import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class DynamicGroupIntegrationTest {
    @Test
    public void testSendingMessageToSingleMember() throws Exception {
        CommLayer commLayer = new UDPMessenger(2440);
        Receiver receiver = mock(Receiver.class);

        DynamicGroup group = new DynamicGroup(commLayer, receiver, 2440, Collections.<Member>emptyList());
        group.broadcast("Hello");

        verify(receiver).receive("Hello");
    }

    @Test
    public void testBuildingAGroup() throws Exception {
        List<Member> members = TestUtils.createMembersOnLocalhost(3);

        Receiver receiver1 = mock(Receiver.class);
        Receiver receiver2 = mock(Receiver.class);
        Receiver receiver3 = mock(Receiver.class);

        DynamicGroup group1 = new DynamicGroup(new UDPMessenger(2440), receiver1, 2440, Collections.<Member>emptyList());
        DynamicGroup group2 = new DynamicGroup(new UDPMessenger(2441), receiver2, 2441, Collections.singletonList(members.get(0)));

        group2.broadcast("Hello");
        verify(receiver1).receive("Hello");
        verify(receiver2).receive("Hello");

        DynamicGroup group3 = new DynamicGroup(new UDPMessenger(2442), receiver3, 2442, Collections.singletonList(members.get(1)));
        group3.broadcast("Goodbye");
        verify(receiver1).receive("Goodbye");
        verify(receiver2).receive("Goodbye");
        verify(receiver3).receive("Goodbye");
    }
}