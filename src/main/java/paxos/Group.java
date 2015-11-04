package paxos;

import paxos.communication.CommLayer;
import paxos.communication.Tick;
import paxos.communication.UDPMessenger;

import java.io.Serializable;

/**
 * This is the basic totally ordered reliable broadcast implementation. It has static membership and it doesn't
 * fragment messages. If the underlying communication layer doesn't fragment messages either, it
 * will keep on failing to transmit. In order to support larger messages you can use the
 * {@link paxos.fragmentation.FragmentingMessenger} along with this class.
 * {@link paxos.fragmentation.FragmentingGroup} might be a better choice because it should deal better
 * with unreliable communication.
 *
 * This class does not persist state, thus it doesn't support recovery of members.
 *
 * @see paxos.dynamic.DynamicGroup
 * @see paxos.fragmentation.FragmentingGroup
 **/
public class Group implements CommLayer.MessageListener {
    private final AcceptorLogic acceptorLogic;
    private final LeaderLogic leaderLogic;
    private final FailureDetector failureDetector;
    private final GroupMembership membership;
    private final CommLayer commLayer;

    // TODO hide commLayer from the user

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
        commLayer.close();
    }

    private void dispatch(Serializable message) {
        leaderLogic.dispatch(message);
        acceptorLogic.dispatch(message);
        failureDetector.dispatch(message);
    }

    public void receive(byte[] message) {
        dispatch((Serializable) PaxosUtils.deserialize(message));
    }
}
