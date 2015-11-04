package paxos.messages;

import java.io.Serializable;

/**
 * Used by a newly elected leader to fill sequence numbering gaps from the previous leader.
 */
public class NoOp implements Serializable {
    @Override
    public boolean equals(Object obj) {
        return obj instanceof NoOp;
    }
}
