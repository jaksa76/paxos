package paxos;

import java.io.IOException;
import java.io.Serializable;

public class Group {
    private final AcceptorLogic acceptorLogic;
    private final LeaderLogic leaderLogic;
    private final Messenger messenger;
    private final FailureDetector failureDetector;
    private final Thread receiverThread;

    private boolean running = true;

    public Group(Messenger messenger, Receiver receiver) throws IOException {
        this.messenger = messenger;

        leaderLogic = new LeaderLogic(messenger);
        acceptorLogic = new AcceptorLogic(messenger, receiver);
        failureDetector = new FailureDetector(messenger, leaderLogic);

        // start receiving messages
        receiverThread = new Thread() {
            @Override
            public void run() {
                try {
                    while (running) {
                        dispatch(Group.this.messenger.receive());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        receiverThread.start();

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
        receiverThread.interrupt();
        messenger.close();
    }

    private void dispatch(Serializable message) {
        leaderLogic.dispatch(message);
        acceptorLogic.dispatch(message);
        failureDetector.dispatch(message);
    }

    public int getPositionInGroup() {
        return messenger.getPositionInGroup();
    }
}
