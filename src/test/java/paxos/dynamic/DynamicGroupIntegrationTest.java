package paxos.dynamic;

import org.junit.After;
import org.junit.Test;
import paxos.*;
import paxos.communication.CommLayer;
import paxos.communication.Member;
import paxos.communication.Members;
import paxos.communication.UDPMessenger;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class DynamicGroupIntegrationTest {
    private Set<DynamicGroup> groups = new HashSet<DynamicGroup>();

    @After public void tearDown() {
        for (DynamicGroup group : groups) {
            group.close();
        }
    }

    @Test
    public void testSendingMessageToSingleMember() throws Exception {
        Receiver receiver = mock(Receiver.class);

        DynamicGroup group = createGroup(receiver, 2440, Collections.<Member>emptyList());
        group.broadcast("Hello");

        verify(receiver).receive("Hello");
    }

    @Test
    public void testBuildingAGroup() throws Exception {
        List<Member> members = TestUtils.createMembersOnLocalhost(3);

        Receiver receiver1 = mock(Receiver.class);
        Receiver receiver2 = mock(Receiver.class);
        Receiver receiver3 = mock(Receiver.class);

        DynamicGroup group1 = createGroup(receiver1, 2440, Collections.<Member>emptyList());
        DynamicGroup group2 = createGroup(receiver2, 2441, Collections.singletonList(members.get(0)));

        group2.broadcast("Hello");
        verify(receiver1, timeout(1000)).receive("Hello");
        verify(receiver2, timeout(1000)).receive("Hello");

        DynamicGroup group3 = createGroup(receiver3, 2442, Collections.singletonList(members.get(1)));
        group3.broadcast("Goodbye");
        verify(receiver1, timeout(1000)).receive("Goodbye");
        verify(receiver2, timeout(1000)).receive("Goodbye");
        verify(receiver3, timeout(1000)).receive("Goodbye");

        verifyNoMoreInteractions(receiver1, receiver2, receiver3);
    }

    @Test
    public void testRemovingMembers() throws Exception {
        List<Member> members = TestUtils.createMembersOnLocalhost(3);

        Receiver receiver1 = mock(Receiver.class);
        Receiver receiver2 = mock(Receiver.class);
        Receiver receiver3 = mock(Receiver.class);

        DynamicGroup group1 = createGroup(receiver1, 2440, Collections.<Member>emptyList());
        DynamicGroup group2 = createGroup(receiver2, 2441, Collections.singletonList(members.get(0)));
        DynamicGroup group3 = createGroup(receiver3, 2442, Collections.singletonList(members.get(1)));
        Thread.sleep(1000);

        group3.broadcast("Hello");
        verify(receiver1, timeout(1000)).receive("Hello");
        verify(receiver2, timeout(1000)).receive("Hello");
        verify(receiver3, timeout(1000)).receive("Hello");

        group2.removeMember(members.get(2));

        Thread.sleep(1000); // wait till the group membership has taken effect

        group2.broadcast("Goodbye");
        verify(receiver1, timeout(1000)).receive("Goodbye");
        verify(receiver2, timeout(1000)).receive("Goodbye");

        verifyNoMoreInteractions(receiver1, receiver2, receiver3);
    }

    private DynamicGroup createGroup(Receiver receiver1, int port, List<Member> knownMembers) throws IOException, InterruptedException {
        DynamicGroup group = new DynamicGroup(new UDPMessenger(port), receiver1, port, knownMembers);
        groups.add(group);
        return group;
    }
}