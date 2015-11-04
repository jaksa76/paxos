package paxos;

import paxos.messages.NoOp;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Receives unordered messages with sequence numbers and invokes the receiver in the correct
 * represented by the sequence number.
 */
public class BufferedReceiver {
    private final Receiver receiver;
    private Map<Long, Serializable> receiverBuffer = new HashMap<Long, Serializable>();

    private long receivedNo = -1;

    /**
     * @param receiver the receiver to invoke when the next message is available.
     */
    public BufferedReceiver(Receiver receiver) {
        this.receiver = receiver;
    }

    /**
     * Notify this receiver that a message has been received. If this is the next message expected by
     * the upstream receiver it will deliver it along with subsequent messages.
     *
     * @param seqNo
     * @param message
     */
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
