package paxos.fragmentation;

import org.hamcrest.CoreMatchers;
import org.junit.Test;
import paxos.PaxosUtils;

import java.io.Serializable;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class FragmentCollectorTest {

    public static final byte[] MSG = PaxosUtils.serialize("The quick brown fox jumps over the lazy dog.");

    @Test
    public void testCollectingOneFragment() throws Exception {
        FragmentCollector collector = new FragmentCollector(1);

        byte[][] fragments = fragment(MSG, 100);

        collector.addPart(0, fragments[0]);
        assertThat(collector.isComplete(), is(true));
        assertThat(collector.extractMessage(), equalTo(MSG));
    }

    @Test
    public void testCollectingTwoFragment() throws Exception {
        FragmentCollector collector = new FragmentCollector(2);

        byte[][] fragments = fragment(MSG, 30);

        collector.addPart(0, fragments[0]);
        assertThat(collector.isComplete(), is(false));
        collector.addPart(1, fragments[1]);
        assertThat(collector.isComplete(), is(true));
        assertThat(collector.extractMessage(), equalTo(MSG));
    }

    @Test
    public void testCollectingThreeFragment() throws Exception {
        FragmentCollector collector = new FragmentCollector(3);

        byte[][] fragments = fragment(MSG, 20);

        collector.addPart(2, fragments[2]);
        assertThat(collector.isComplete(), is(false));
        collector.addPart(1, fragments[1]);
        assertThat(collector.isComplete(), is(false));
        collector.addPart(0, fragments[0]);
        assertThat(collector.isComplete(), is(true));
        assertThat(collector.extractMessage(), equalTo(MSG));
    }

    @Test
    public void testReceivingTwiceTheSameFragment() throws Exception {
        FragmentCollector collector = new FragmentCollector(3);

        byte[][] fragments = fragment(MSG, 20);

        collector.addPart(2, fragments[2]);
        assertThat(collector.isComplete(), is(false));
        collector.addPart(1, fragments[1]);
        assertThat(collector.isComplete(), is(false));
        collector.addPart(1, fragments[1]);
        assertThat(collector.isComplete(), is(false));
        collector.addPart(0, fragments[0]);
        assertThat(collector.isComplete(), is(true));

        assertThat(collector.extractMessage(), equalTo(MSG));
    }


    private byte[][] fragment(byte[] bytes, int size) {
        byte[][] parts = new byte[(int) Math.ceil(bytes.length/((double)size))][];
        for (int i = 0; i < parts.length; i++) {
            int offset = i*size;
            int remainingBytes = bytes.length - offset;
            int fragmentLength = Math.min(remainingBytes, size);
            parts[i] = new byte[fragmentLength];
            System.arraycopy(bytes, offset, parts[i], 0, fragmentLength);
        }
        return parts;
    }
}