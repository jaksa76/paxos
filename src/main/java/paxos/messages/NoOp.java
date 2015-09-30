package paxos.messages;

import java.io.Serializable;

public class NoOp implements Serializable {
    @Override
    public boolean equals(Object obj) {
        return obj instanceof NoOp;
    }
}
