package paxos;

import org.junit.Assert;
import org.junit.Test;
import paxos.messages.Accept;

import java.net.InetAddress;

import static org.junit.Assert.assertEquals;

public class SerializationTest {
    @Test
    public void testSerialization() throws Exception {
        Accept original = new Accept(1l, 2l, "hello", 1l, new Member(InetAddress.getLocalHost(), 1234));
        Accept clone = (Accept) PaxosUtils.deserialize(PaxosUtils.serialize(original));

        assertEquals(1l, clone.viewNo);
        assertEquals(2l, clone.seqNo);
        assertEquals("hello", clone.message);
    }
}
