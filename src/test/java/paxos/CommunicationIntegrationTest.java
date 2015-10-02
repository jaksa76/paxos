package paxos;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.lang.Math.random;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static paxos.TestUtils.*;

public class CommunicationIntegrationTest {
    private final Set<Group> groups = new HashSet<Group>();

    @After public void tearDown() throws InterruptedException {
        for (Group group : groups) {
            group.close();
        }
        Thread.sleep(500);
    }

    @Test
    public void testBroadcast() throws Exception {
        Receiver receiver = Mockito.mock(Receiver.class);
        List<Member> members = createMembersOnLocalhost(3);
        Group group1 = new Group(new UDPMessenger(members, 2440), null);
        Group group2 = new Group(new UDPMessenger(members, 2441), receiver);
        Group group3 = new Group(new UDPMessenger(members, 2442), null);
        groups.addAll(asSet(group1, group2, group3));

        Thread.sleep(500);

        group1.broadcast("Hello");

        Thread.sleep(100);

        verify(receiver).receive(eq("Hello"));
    }

    @Test
    public void testNoMessageLossOrDuplication() throws Exception {
        int groupSize = 5;
        List<Member> members = createMembersOnLocalhost(groupSize);
        Receiver[] receivers = createReceivers(groupSize);
        Group[] endpoints = createEndpoints(members, receivers);

        long start = System.currentTimeMillis();

        Thread[] senders = createSenders(endpoints);
        waitTillFinished(senders);

        double delta = (System.currentTimeMillis() - start) / 1000.0; // in seconds
        System.out.println("time taken: " + delta);
        System.out.println("req/s: " + (5000.0 / delta));

        for (Receiver receiver : receivers) {
            CountingReceiver countingReceiver = (CountingReceiver) receiver;
            findDuplicates(countingReceiver.messages);
            Assert.assertEquals(5000, countingReceiver.msgCount);
        }
    }

    @Test public void testSequentiality() throws Exception {
        int groupSize = 3;
        List<Member> members = createMembersOnLocalhost(groupSize);
        Receiver[] receivers = createCalculatingReceivers(groupSize);
        Group[] endpoints = createEndpoints(members, receivers);

        Thread[] senders = createCalculatingSenders(endpoints);
        waitTillFinished(senders);

        double expected = ((CalculatingReceiver) receivers[0]).value;
        for (int i = 1; i < groupSize; i++) {
            System.out.println(expected);
            Assert.assertEquals(expected, ((CalculatingReceiver) receivers[i]).value, expected * .001);
        }
    }

    @Test public void testKillingTheLeader() throws Exception {
        int groupSize = 5;
        List<Member> members = createMembersOnLocalhost(groupSize);
        Receiver[] receivers = createReceivers(groupSize);
        Group[] endpoints = createEndpoints(members, receivers);
        Group leader = endpoints[groupSize-1];

        Thread[] senders = new Thread[endpoints.length-1];
        for (int i = 0; i < senders.length; i++) {
            senders[i] = new Sender(i, endpoints[i]);
            senders[i].start();
        }

        Thread.sleep(500);
        leader.close();

        waitTillFinished(senders);

        for (int i = 1; i < receivers.length - 1; i++) {
            System.out.println(((CountingReceiver) receivers[i]).msgCount);
            Assert.assertEquals(4000, ((CountingReceiver) receivers[i]).msgCount);
        }
    }

    @Test public void testKillingTheLeader2() throws Exception {
        int groupSize = 3;
        List<Member> members = createMembersOnLocalhost(groupSize);
        Receiver[] receivers = createMockReceivers(groupSize);
        Group[] endpoints = createEndpoints(members, receivers);

        endpoints[0].broadcast("Hello");
        verify(receivers[1]).receive(eq("Hello"));

        endpoints[groupSize-1].close();
        endpoints[0].broadcast("Goodbye");

        verify(receivers[1], timeout(1000)).receive(eq("Goodbye"));
    }

    private Group[] createEndpoints(List<Member> members, Receiver[] receivers) throws IOException, InterruptedException {
        final Group[] endpoints = new Group[members.size()];
        for (int i = 0; i < members.size(); i++) {
            endpoints[i] = new Group(new UDPMessenger(members, members.get(i).getPort()), receivers[i]);
            groups.add(endpoints[i]);
        }
        Thread.sleep(500); // allow some time for leader election
        return endpoints;
    }

    private Receiver[] createReceivers(int groupSize) {
        Receiver[] receivers = new Receiver[groupSize];
        for (int i = 0; i < groupSize; i++) receivers[i] = new CountingReceiver();
        return receivers;
    }

    private Receiver[] createMockReceivers(int groupSize) {
        Receiver[] receivers = new Receiver[groupSize];
        for (int i = 0; i < groupSize; i++) receivers[i] = Mockito.mock(Receiver.class);
        return receivers;
    }

    private Receiver[] createCalculatingReceivers(int groupSize) {
        Receiver[] receivers = new Receiver[groupSize];
        for (int i = 0; i < groupSize; i++) receivers[i] = new CalculatingReceiver();
        return receivers;
    }

    private Thread[] createSenders(Group[] endpoints) {
        Thread[] senders = new Thread[endpoints.length];
        for (int i = 0; i < endpoints.length; i++) {
            senders[i] = new Sender(i, endpoints[i]);
            senders[i].start();
        }
        return senders;
    }

    private Thread[] createCalculatingSenders(Group[] endpoints) {
        Thread[] senders = new Thread[endpoints.length];
        for (int i = 0; i < endpoints.length; i++) {
            senders[i] = new CalculatingSender(endpoints[i]);
            senders[i].start();
        }
        return senders;
    }


    public static class CalculatingReceiver implements Receiver {
        public double value = 1.0;
        public long messages = 0l;
        private long start = System.currentTimeMillis();
        public synchronized void receive(Serializable message) {
            if (message.equals("+1")) value += 1.0;
            else value *= 1.01;
            messages++;
            if (messages % 10000 == 0) {
                System.out.println("received " + messages + " msgCount");
                long delta = System.currentTimeMillis() - start;
                System.out.println("speed " + ((float) messages) / delta * 1000.0);
            }
        }
    }

    public static class CountingReceiver implements Receiver {
        public long msgCount = 0l;
        public List<String> messages = new ArrayList<String>();
        public synchronized void receive(Serializable message) {
            msgCount++;
            messages.add((String) message);
        }
    }

    private static class Sender extends Thread {
        private final int i;
        private final Group endpoint;
        public Sender(int i, Group endpoint) {
            this.i = i;
            this.endpoint = endpoint;
        }

        @Override
        public void run() {
            for (int j = 0; j < 1000; j++) {
                try {
                    String messageToSend = i + "_" + j;
                    endpoint.broadcast(messageToSend);
                } catch (Exception e) { e.printStackTrace(); }
            }
        }
    }

    private static class CalculatingSender extends Thread {
        private final Group endpoint;
        public CalculatingSender(Group endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public void run() {
            for (int i = 0; i < 1000; i++) {
                Serializable messageToSend = random() < .5 ? "+1" : "*2";
                endpoint.broadcast(messageToSend);
            }
        }
    }

    private void findDuplicates(List<String> messages) {
        Set<String> messagesFound = new HashSet<String>();
        if (messages.size() != 5000) {
            // find duplicate
            for (String msg : messages) {
                if (messagesFound.contains(msg)) {
                    System.out.println("duplicate message " + msg);
                }
                messagesFound.add(msg);
            }
        }
    }
}
