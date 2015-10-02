package paxos;

import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import paxos.messages.NoOp;

import static org.junit.Assert.*;

public class BufferedReceiverTest {
    @Test
    public void testReceivingFirstMessage() throws Exception {
        Receiver receiver = Mockito.mock(Receiver.class);

        BufferedReceiver bufferedReceiver = new BufferedReceiver(receiver);
        bufferedReceiver.receive(0l, "hi");

        Mockito.verify(receiver).receive("hi");
    }

    @Test
    public void testReceivingManyMessages() throws Exception {
        Receiver receiver = Mockito.mock(Receiver.class);

        BufferedReceiver bufferedReceiver = new BufferedReceiver(receiver);
        bufferedReceiver.receive(0l, "hi 1");
        Mockito.verify(receiver).receive("hi 1");

        bufferedReceiver.receive(1l, "hi 2");
        Mockito.verify(receiver).receive("hi 2");

        bufferedReceiver.receive(2l, "hi 3");
        Mockito.verify(receiver).receive("hi 3");
    }


    @Test
    public void testFilteringOutNoOpMessage() throws Exception {
        Receiver receiver = Mockito.mock(Receiver.class);

        BufferedReceiver bufferedReceiver = new BufferedReceiver(receiver);
        bufferedReceiver.receive(0l, new NoOp());
        Mockito.verifyNoMoreInteractions(receiver);

        bufferedReceiver.receive(1l, "hi"); // this should be delivered only after 2
        Mockito.verify(receiver).receive("hi");

        Mockito.verifyNoMoreInteractions(receiver);
    }

    @Test
    public void testReceivingMessagesOutOfOrder() throws Exception {
        Receiver receiver = Mockito.mock(Receiver.class);

        BufferedReceiver bufferedReceiver = new BufferedReceiver(receiver);
        bufferedReceiver.receive(0l, "hi 1");
        Mockito.verify(receiver).receive("hi 1");

        bufferedReceiver.receive(2l, "hi 3"); // this should be delivered only after 2
        bufferedReceiver.receive(4l, "hi 5"); // this should not be delivered
        Mockito.verifyNoMoreInteractions(receiver);

        bufferedReceiver.receive(1l, "hi 2");
        Mockito.verify(receiver).receive("hi 2");
        Mockito.verify(receiver).receive("hi 3");

        Mockito.verifyNoMoreInteractions(receiver);
    }
}