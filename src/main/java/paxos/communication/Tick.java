package paxos.communication;

import java.io.Serializable;

public class Tick implements Serializable {
    public final long time;

    public Tick(long time) {
        this.time = time;
    }
}
