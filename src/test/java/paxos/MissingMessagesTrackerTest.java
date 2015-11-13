package paxos;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.Assert.*;

public class MissingMessagesTrackerTest {
    @Test
    public void testInitialState() throws Exception {
        MissingMessagesTracker tail = new MissingMessagesTracker();
        assertEquals(tail.getMissing(0), Collections.<Long>emptySet());
        assertEquals(tail.getMissing(1), Collections.singleton(0l));
    }

    @Test
    public void testOneMessage() throws Exception {
        MissingMessagesTracker tail = new MissingMessagesTracker();
        tail.received(0);
        assertEquals(tail.getMissing(1), Collections.<Long>emptySet());
    }

    @Test
    public void testOneMessageThenGap() throws Exception {
        MissingMessagesTracker tail = new MissingMessagesTracker();
        tail.received(0);
        assertEquals(tail.getMissing(2), Collections.singleton(1l));
    }

    @Test
    public void testContiguousSequence() throws Exception {
        MissingMessagesTracker tail = new MissingMessagesTracker();
        tail.received(0);
        tail.received(1);
        tail.received(2);
        assertEquals(tail.getMissing(3), Collections.<Long>emptySet());
    }

    @Test
    public void testContiguousSequenceThenGap() throws Exception {
        MissingMessagesTracker tail = new MissingMessagesTracker();
        tail.received(0);
        tail.received(1);
        tail.received(2);
        assertEquals(tail.getMissing(4), Collections.singleton(3l));
    }

    @Test
    public void testContiguousSequenceThenWideGap() throws Exception {
        MissingMessagesTracker tail = new MissingMessagesTracker();
        tail.received(0);
        tail.received(1);
        tail.received(2);
        assertEquals(tail.getMissing(6), new HashSet<Long>(Arrays.asList(3l, 4l, 5l)));
    }

    @Test
    public void testSequenceWithHoles() throws Exception {
        MissingMessagesTracker tail = new MissingMessagesTracker();
        tail.received(0);
        // 1 is missing
        tail.received(2);
        assertEquals(tail.getMissing(3), Collections.singleton(1l));
    }

    @Test
    public void testOutOfOrder() throws Exception {
        MissingMessagesTracker tail = new MissingMessagesTracker();
        tail.received(0);
        tail.received(2);
        tail.received(1);
        assertEquals(tail.getMissing(3), Collections.<Long>emptySet());
    }


    @Test
    public void testABitOfEverything() throws Exception {
        MissingMessagesTracker tail = new MissingMessagesTracker();
        tail.received(0);
        tail.received(2);
        tail.received(3);
        tail.received(1);
        tail.received(4);
        assertEquals(tail.getMissing(7), new HashSet<Long>(Arrays.asList(5l, 6l)));
    }
}