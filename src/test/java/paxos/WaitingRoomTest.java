package paxos;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

public class WaitingRoomTest {
    @Test
    public void testWaitingAfterUnblocked() throws Exception {
        final WaitingRoom waitingRoom = new WaitingRoom();
        final Receiver receiver = Mockito.mock(Receiver.class);

        Thread thread1 = new Thread() {
            @Override
            public void run() {
                try {
                    boolean released = waitingRoom.waitALittle(1l);
                    if (released) receiver.receive("released");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        waitingRoom.unblock(1l);

        thread1.start();

        verify(receiver, timeout(1200)).receive("released");
    }

    @Test
    public void testWaitingBeforeUnblocked() throws Exception {
        final WaitingRoom waitingRoom = new WaitingRoom();
        final Receiver receiver = Mockito.mock(Receiver.class);

        Thread thread1 = new Thread() {
            @Override
            public void run() {
                try {
                    boolean released = waitingRoom.waitALittle(1l);
                    if (released) receiver.receive("released");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        thread1.start();
        Thread.sleep(100);

        waitingRoom.unblock(1l);

        verify(receiver, timeout(2000)).receive("released");
    }
}