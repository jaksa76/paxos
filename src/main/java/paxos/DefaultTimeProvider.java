package paxos;

public class DefaultTimeProvider implements TimeProvider {
    public long getTime() {
        return System.currentTimeMillis();
    }
}
