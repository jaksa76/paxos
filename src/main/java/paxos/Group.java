package paxos;

import paxos.communication.CommLayer;
import paxos.communication.Tick;
import paxos.communication.UDPMessenger;

import java.io.Serializable;

public class Group implements UDPMessenger.MessageListener {
    private final AcceptorLogic acceptorLogic;
    private final LeaderLogic leaderLogic;
    private final FailureDetector failureDetector;
    private final GroupMembership membership;
    private final CommLayer commLayer;

    private boolean running = true;

    public Group(GroupMembership membership, CommLayer commLayer, Receiver receiver) {
        this(membership, commLayer, receiver, System.currentTimeMillis());
    }

    public Group(GroupMembership membership, CommLayer commLayer, Receiver receiver, long time) {
        this.membership = membership;
        this.commLayer = commLayer;

        leaderLogic = new LeaderLogic(membership, commLayer, time);
        acceptorLogic = new AcceptorLogic(membership, commLayer, receiver);
        failureDetector = new FailureDetector(membership, commLayer, leaderLogic);

        this.commLayer.setListener(this);
    }

    public void broadcast(Serializable message) {
        acceptorLogic.broadcast(message);
    }

    public void close() {
        this.running = false;
        commLayer.close();
    }

    private void dispatch(Serializable message) {
        leaderLogic.dispatch(message);
        acceptorLogic.dispatch(message);
        failureDetector.dispatch(message);
    }

    public int getPositionInGroup() {
        return membership.getPositionInGroup();
    }

    public void receive(byte[] message) {
        dispatch((Serializable) PaxosUtils.deserialize(message));
    }
}
