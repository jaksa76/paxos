package paxos;

import paxos.messages.NoOp;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class BufferedReceiver {
    private final Receiver receiver;
    private Map<Long, Serializable> receiverBuffer = new HashMap<Long, Serializable>();

    private long receivedNo = -1;

    public BufferedReceiver(Receiver receiver) {
        this.receiver = receiver;
    }

    public void receive(long seqNo, Serializable message) {
        if (receiver != null) {
            receiverBuffer.put(seqNo, message);
            while (receiverBuffer.containsKey(receivedNo + 1)) {
                receivedNo++;
                Serializable messageToDeliver = receiverBuffer.get(receivedNo);
                if (!(messageToDeliver instanceof NoOp)) receiver.receive(messageToDeliver);
                receiverBuffer.remove(receivedNo);
            }
        }
    }
}
