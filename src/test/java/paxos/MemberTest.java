package paxos;

import org.junit.Test;
import paxos.Member;

import java.net.InetAddress;

import static org.junit.Assert.*;

public class MemberTest {
    @Test public void testComparisons() throws Exception {
        Member m1 = new Member(InetAddress.getByName("192.168.0.1"), 1);
        Member m2 = new Member(InetAddress.getByName("192.168.0.2"), 1);
        Member m3 = new Member(InetAddress.getByName("192.168.0.3"), 1);
        Member m4 = new Member(InetAddress.getByName("192.168.0.3"), 2);
        Member m5 = new Member(InetAddress.getByName("192.168.0.3"), 2);

        assertTrue(m1.compareTo(m2) < 0);
        assertTrue(m2.compareTo(m1) > 0);
        assertTrue(m2.compareTo(m3) < 0);
        assertTrue(m3.compareTo(m4) < 0);
        assertTrue(m4.compareTo(m5) == 0);
    }
}
