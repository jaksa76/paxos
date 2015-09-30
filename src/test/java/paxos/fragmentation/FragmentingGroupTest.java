package paxos.fragmentation;

import org.junit.Test;
import org.mockito.Mockito;
import paxos.*;
import paxos.messages.SpecialMessage;

import java.io.Serializable;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class FragmentingGroupTest {
    @Test
    public void testNotFragmentingMessage() throws Exception {
        Group underlyingGroup = mock(Group.class);
        FragmentingGroup group = new FragmentingGroup(underlyingGroup);
        group.broadcast(createMessageOfLength(60000));

        verify(underlyingGroup, times(1)).broadcast(Mockito.<Serializable>any());
    }

    @Test
    public void testFragmentingMessage() throws Exception {
        Group underlyingGroup = mock(Group.class);
        FragmentingGroup group = new FragmentingGroup(underlyingGroup);
        group.broadcast(createMessageOfLength(3*64000 + 100));

        verify(underlyingGroup, times(4)).broadcast(Mockito.<Serializable>any());
    }

    private Serializable createMessageOfLength(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) (i%256);
        }
        return bytes;
    }
}