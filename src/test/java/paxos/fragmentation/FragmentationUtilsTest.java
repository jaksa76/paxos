package paxos.fragmentation;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class FragmentationUtilsTest {
    @Test
    public void testFragmentationWithOneFragment() throws Exception {
        MessageFragment[] messageFragments = FragmentationUtils.performFragmentation("The quick brown fox jumps over the lazy dog.", 1, 100);
        assertThat(messageFragments.length, is(1));
    }

    @Test
    public void testFragmentationWithTwoFragments() throws Exception {
        MessageFragment[] messageFragments = FragmentationUtils.performFragmentation("The quick brown fox jumps over the lazy dog.", 1, 30);
        assertThat(messageFragments.length, is(2));
    }

    @Test
    public void testFragmentationWithThreeFragments() throws Exception {
        MessageFragment[] messageFragments = FragmentationUtils.performFragmentation("The quick brown fox jumps over the lazy dog.", 1, 20);
        assertThat(messageFragments.length, is(3));
    }

}