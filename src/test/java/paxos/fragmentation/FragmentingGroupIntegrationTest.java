package paxos.fragmentation;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import paxos.communication.Member;
import paxos.Receiver;
import paxos.communication.UDPMessenger;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static paxos.TestUtils.*;

public class FragmentingGroupIntegrationTest {
    private final Set<FragmentingGroup> groups = new HashSet<FragmentingGroup>();
    private static final byte[] MESSAGE_TO_SEND = createMessageOfLength(64000*3+100);
//    private static final byte[] MESSAGE_TO_SEND = createMessageOfLength(100000);
    private static final int MESSAGES_TO_SEND = 3;
    private static final int GROUP_SIZE = 3;

    @After
    public void tearDown() {
        for (FragmentingGroup group : groups) {
            group.close();
        }
    }


    @Test
    public void testTransmittingShortMessage() throws Exception {
        Receiver receiver = Mockito.mock(Receiver.class);
        List<Member> members = createMembersOnLocalhost(3);
        FragmentingGroup group1 = new FragmentingGroup(createMembership(members, 0), new UDPMessenger(2440), null);
        FragmentingGroup group2 = new FragmentingGroup(createMembership(members, 1), new UDPMessenger(2441), receiver);
        FragmentingGroup group3 = new FragmentingGroup(createMembership(members, 2), new UDPMessenger(2442), null);
        groups.addAll(asSet(group1, group2, group3));

        Thread.sleep(500);

        group1.broadcast("Hello");

        Thread.sleep(100);

        verify(receiver).receive(eq("Hello"));
    }

    @Test
    public void testTransmittingLongMessage() throws Exception {
        Receiver receiver = Mockito.mock(Receiver.class);
        List<Member> members = createMembersOnLocalhost(3);
        FragmentingGroup group1 = new FragmentingGroup(createMembership(members, 0), new UDPMessenger(2440), null);
        FragmentingGroup group2 = new FragmentingGroup(createMembership(members, 1), new UDPMessenger(2441), receiver);
        FragmentingGroup group3 = new FragmentingGroup(createMembership(members, 2), new UDPMessenger(2442), null);
        groups.addAll(asSet(group1, group2, group3));

        Thread.sleep(500);

        group1.broadcast(MESSAGE_TO_SEND);

        Thread.sleep(100);

        verify(receiver).receive(eq(MESSAGE_TO_SEND));
    }

    @Test
    public void testTransmittingLongMessages() throws Exception {
        List<Member> members = createMembersOnLocalhost(GROUP_SIZE);
        Receiver[] receivers = createReceivers(GROUP_SIZE);
        FragmentingGroup[] endpoints = createEndpoints(members, receivers);

        long start = System.currentTimeMillis();

        Thread[] senders = createSenders(endpoints);
        waitTillFinished(senders);

        double delta = (System.currentTimeMillis() - start) / 1000.0; // in seconds
        System.out.println("time taken: " + delta);
        System.out.println("req/s: " + (MESSAGES_TO_SEND * GROUP_SIZE / delta));

        for (Receiver receiver : receivers) {
            CountingReceiver countingReceiver = (CountingReceiver) receiver;
            Assert.assertEquals(MESSAGES_TO_SEND * GROUP_SIZE, countingReceiver.msgCount);
        }
    }


    private FragmentingGroup[] createEndpoints(List<Member> members, Receiver[] receivers) throws IOException, InterruptedException {
        final FragmentingGroup[] endpoints = new FragmentingGroup[members.size()];
        for (int i = 0; i < members.size(); i++) {
            endpoints[i] = new FragmentingGroup(createMembership(members, i), new UDPMessenger(members.get(i).getPort()), receivers[i]);
            groups.add(endpoints[i]);
        }
        Thread.sleep(500); // allow some time for leader election
        return endpoints;
    }

    public static class CountingReceiver implements Receiver {
        public long msgCount = 0l;
        public synchronized void receive(Serializable message) {
            System.out.println(".");
            msgCount++;
        }
    }

    private Thread[] createSenders(FragmentingGroup[] endpoints) {
        Thread[] senders = new Thread[endpoints.length];
        for (int i = 0; i < endpoints.length; i++) {
            senders[i] = new Sender(i, endpoints[i]);
            senders[i].start();
        }
        return senders;
    }

    private static class Sender extends Thread {

        private final int i;
        private final FragmentingGroup endpoint;
        public Sender(int i, FragmentingGroup endpoint) {
            this.i = i;
            this.endpoint = endpoint;
        }

        @Override
        public void run() {
            for (int j = 0; j < MESSAGES_TO_SEND; j++) {
                try {

                    endpoint.broadcast(MESSAGE_TO_SEND);
                } catch (Exception e) { e.printStackTrace(); }
            }
        }

    }

    private void findDuplicates(List<String> messages) {
        Set<String> messagesFound = new HashSet<String>();
        if (messages.size() != MESSAGES_TO_SEND * GROUP_SIZE) {
            // find duplicate
            for (String msg : messages) {
                if (messagesFound.contains(msg)) {
                    System.out.println("duplicate message " + msg);
                }
                messagesFound.add(msg);
            }
        }
    }

    private Receiver[] createReceivers(int groupSize) {
        Receiver[] receivers = new Receiver[groupSize];
        for (int i = 0; i < groupSize; i++) receivers[i] = new CountingReceiver();
        return receivers;
    }

    static private byte[] createMessageOfLength(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) (i%256);
        }
        return bytes;
    }
}
