package paxos;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class ClientProxy {
    private final HashMap<Long, Semaphore> waitingForResponse = new HashMap<Long, Semaphore>();
    private final Member leader;
    private final AcceptorLogic acceptor;

    public ClientProxy(AcceptorLogic acceptor, Member leader) {
        this.acceptor = acceptor;
        this.leader = leader;
    }

    public void broadcast(Serializable message) throws IOException {
        long msgId = System.currentTimeMillis() * 100 + message.hashCode();
        Semaphore semaphore = new Semaphore(0);
        synchronized (waitingForResponse) {
            waitingForResponse.put(msgId, semaphore);
        }
        boolean broadcastSuccessfull = false;
        try {
            while (!broadcastSuccessfull) {
//                acceptor.broadcast(message, msgId);
                broadcastSuccessfull = semaphore.tryAcquire(1000, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void unblock(long msgId) {
        synchronized (waitingForResponse) {
            Semaphore semaphore = waitingForResponse.get(msgId);
            if (semaphore != null) semaphore.release();
        }
    }
}
