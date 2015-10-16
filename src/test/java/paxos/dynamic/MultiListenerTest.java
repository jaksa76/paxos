package paxos.dynamic;

import org.junit.Test;
import org.mockito.Mockito;
import paxos.CommLayer;

import static org.junit.Assert.*;

public class MultiListenerTest {

    public static final byte[] MESSAGE = "Hello".getBytes();

    @Test
    public void testReceivingAMessage() throws Exception {
        CommLayer.MessageListener listener1 = Mockito.mock(CommLayer.MessageListener.class);
        CommLayer.MessageListener listener2 = Mockito.mock(CommLayer.MessageListener.class);
        MultiListener multiListener = new MultiListener();
        multiListener.addListener(listener1);
        multiListener.addListener(listener2);

        multiListener.receive(MESSAGE);

        Mockito.verify(listener1).receive(MESSAGE);
        Mockito.verify(listener2).receive(MESSAGE);
    }
}