package paxos;

import java.io.IOException;
import java.io.Serializable;

public class Group implements UDPMessenger.MessageListener {
    private final AcceptorLogic acceptorLogic;
    private final LeaderLogic leaderLogic;
    private final FailureDetector failureDetector;
    private final GroupMembership membership;
    private final CommLayer commLayer;

    private boolean running = true;

    public Group(GroupMembership membership, CommLayer commLayer, Receiver receiver) {
        this.membership = membership;
        this.commLayer = commLayer;

        leaderLogic = new LeaderLogic(membership, commLayer);
        acceptorLogic = new AcceptorLogic(membership, commLayer, receiver);
        failureDetector = new FailureDetector(membership, commLayer, leaderLogic);

        this.commLayer.setListener(this);

        // TODO check if this causes race conditions
        new Thread() {
            @Override
            public void run() {
                try {
                    while (running) {
                        leaderLogic.update();
                        sleep(1000);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }

    public void broadcast(Serializable message) {
        acceptorLogic.broadcast(message);
    }

    public void close() {
        failureDetector.close();
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
