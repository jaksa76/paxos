package paxos;

import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import paxos.communication.Member;
import paxos.communication.Members;
import paxos.communication.UDPMessenger;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.*;

import static java.lang.Math.random;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static paxos.TestUtils.*;

public class BasicGroupIntegrationTest {
    private final Set<BasicGroup> groups = new HashSet<BasicGroup>();

    @After public void tearDown() throws InterruptedException {
        for (BasicGroup group : groups) {
            group.close();
        }
        Thread.sleep(3000);
        groups.clear();
    }

    @Test
    public void tutorial() throws Exception {
        // this is the list of members
        Members members = new Members(
                new Member(), // this is a reference to a member on the localhost on default port (2440)
                new Member(2441), // this one is on localhost with the specified port
                new Member(InetAddress.getLocalHost(), 2442)); // you can specify the address and port manually

        // we need to define a receiver
        class MyReceiver implements Receiver {
            // we follow a reactive pattern here
            public void receive(Serializable message) {
                System.out.println("received " + message.toString());
            }
        }

        // this actually creates the members
        BasicGroup group1 = new BasicGroup(members.get(0), new MyReceiver());
        BasicGroup group2 = new BasicGroup(members.get(1), new MyReceiver());
        BasicGroup group3 = new BasicGroup(members.get(2), new MyReceiver());

        // this will cause all receivers to print "received Hello"
        group2.broadcast("Hello");

        Thread.sleep(1); // allow the members to receive the message

        group1.close(); group2.close(); group3.close();
    }

    @Test
    public void testBroadcast() throws Exception {
        Receiver receiver = Mockito.mock(Receiver.class);
        List<Member> members = createMembersOnLocalhost(3);
        BasicGroup group1 = new BasicGroup(createMembership(members, 0), new UDPMessenger(2440), null);
        BasicGroup group2 = new BasicGroup(createMembership(members, 1), new UDPMessenger(2441), receiver);
        BasicGroup group3 = new BasicGroup(createMembership(members, 2), new UDPMessenger(2442), null);
        groups.addAll(asSet(group1, group2, group3));

        Thread.sleep(500);

        group1.broadcast("Hello");

        Thread.sleep(100);

        verify(receiver, timeout(1000)).receive(eq("Hello"));
    }

    @Test
    public void testNoMessageLossOrDuplication() throws Exception {
        int groupSize = 5;
        List<Member> members = createMembersOnLocalhost(groupSize);
        Receiver[] receivers = createReceivers(groupSize);
        BasicGroup[] endpoints = createEndpoints(members, receivers);

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
        BasicGroup[] endpoints = createEndpoints(members, receivers);

        Thread[] senders = createCalculatingSenders(endpoints);
        waitTillFinished(senders);

        double expected = ((CalculatingReceiver) receivers[0]).value;
        for (int i = 1; i < groupSize; i++) {
            System.out.println(expected);
            Assert.assertEquals(expected, ((CalculatingReceiver) receivers[i]).value, expected * .001);
        }
    }

    @Test
    @Ignore
    public void testKillingTheLeader() throws Exception {
        int groupSize = 5;
        List<Member> members = createMembersOnLocalhost(groupSize);
        Receiver[] receivers = createReceivers(groupSize);
        BasicGroup[] endpoints = createEndpoints(members, receivers);
        BasicGroup leader = endpoints[groupSize-1];

        Thread[] senders = new Thread[endpoints.length-1];
        for (int i = 0; i < senders.length; i++) {
            senders[i] = new Sender(i, endpoints[i]);
            senders[i].start();
        }

        Thread.sleep(300);
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
        BasicGroup[] endpoints = createEndpoints(members, receivers);

        endpoints[0].broadcast("Hello");
        verify(receivers[1], timeout(1000)).receive(eq("Hello"));

        endpoints[groupSize-1].close();
        endpoints[0].broadcast("Goodbye");

        verify(receivers[1], timeout(1000)).receive(eq("Goodbye"));
    }

    private BasicGroup[] createEndpoints(List<Member> members, Receiver[] receivers) throws IOException, InterruptedException {
        final BasicGroup[] endpoints = new BasicGroup[members.size()];
        for (int i = 0; i < members.size(); i++) {
            endpoints[i] = new BasicGroup(createMembership(members, i), new UDPMessenger(members.get(i).getPort()), receivers[i]);
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

    private Thread[] createSenders(BasicGroup[] endpoints) {
        Thread[] senders = new Thread[endpoints.length];
        for (int i = 0; i < endpoints.length; i++) {
            senders[i] = new Sender(i, endpoints[i]);
            senders[i].start();
        }
        return senders;
    }

    private Thread[] createCalculatingSenders(BasicGroup[] endpoints) {
        Thread[] senders = new Thread[endpoints.length];
        for (int i = 0; i < endpoints.length; i++) {
            senders[i] = new CalculatingSender(endpoints[i]);
            senders[i].start();
        }
        return senders;
    }

    public static class CalculatingReceiver implements Receiver {
        public double value = 1.0;
        public long messages = 0L;
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
        public long msgCount = 0L;
        public List<String> messages = new ArrayList<String>();
        public synchronized void receive(Serializable message) {
            msgCount++;
            messages.add((String) message);
        }
    }

    private static class Sender extends Thread {
        private final int i;
        private final BasicGroup endpoint;
        public Sender(int i, BasicGroup endpoint) {
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
        private final BasicGroup endpoint;
        public CalculatingSender(BasicGroup endpoint) {
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
