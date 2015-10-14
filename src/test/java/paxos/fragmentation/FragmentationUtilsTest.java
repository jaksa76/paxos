package paxos.fragmentation;

import org.hamcrest.CoreMatchers;
import org.junit.Test;
import paxos.PaxosUtils;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class FragmentationUtilsTest {

    public static final byte[] BYTES = PaxosUtils.serialize("The quick brown fox jumps over the lazy dog.");

    @Test
    public void testFragmentationWithOneFragment() throws Exception {
        MessageFragment[] messageFragments = FragmentationUtils.performFragmentation(BYTES, 1, 100);
        assertThat(messageFragments.length, is(1));
    }

    @Test
    public void testFragmentationWithTwoFragments() throws Exception {
        MessageFragment[] messageFragments = FragmentationUtils.performFragmentation(BYTES, 1, 30);
        assertThat(messageFragments.length, is(2));
    }

    @Test
    public void testFragmentationWithThreeFragments() throws Exception {
        MessageFragment[] messageFragments = FragmentationUtils.performFragmentation(BYTES, 1, 20);
        assertThat(messageFragments.length, is(3));
    }

}