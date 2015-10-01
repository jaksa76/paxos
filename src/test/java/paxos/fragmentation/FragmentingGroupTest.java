package paxos.fragmentation;

import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.mockito.Mockito;
import paxos.Group;
import paxos.Receiver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.*;

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
        group.broadcast(createMessageOfLength(3 * 64000 + 100));

        verify(underlyingGroup, times(3)).broadcast(argThat(messageFragment(64000)));
        verify(underlyingGroup).broadcast(argThat(messageFragment(127)));
    }

    @Test
    public void testRecomposingMessage() throws Exception {
        Receiver receiver = mock(Receiver.class);
        FragmentingGroup.JoinerReceiver joinerReceiver = new FragmentingGroup.JoinerReceiver(receiver);

        joinerReceiver.receive(createMessageFragment(1, 0, 2));
        joinerReceiver.receive(createMessageFragment(1, 1, 2));
        joinerReceiver.receive(createMessageFragment(1, 2, 2));

        verify(receiver).receive(eq(createMessageOfLength(200)));
    }


    private MessageFragment createMessageFragment(long id, int i, int parts) throws IOException {
        Serializable message = createMessageOfLength(parts * 100);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(message);
        byte[] allBytes = baos.toByteArray();
        byte[] bytes = Arrays.copyOfRange(allBytes, i * 100, Math.min(i * 100 + 100, allBytes.length));
        return new MessageFragment(id, bytes, i, parts + 1);
    }

    private TypeSafeMatcher<Serializable> messageFragment(final int lenght) {
        return new CustomTypeSafeMatcher<Serializable>("message of lenght " + lenght) {
            @Override
            protected boolean matchesSafely(Serializable serializable) {
                if (serializable instanceof MessageFragment) {
                    MessageFragment messageFragment = (MessageFragment) serializable;
                    return messageFragment.part.length == lenght;
                }
                return false;
            }
        };
    }

    private byte[] createMessageOfLength(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) (i%256);
        }
        return bytes;
    }

}