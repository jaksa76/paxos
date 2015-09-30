package paxos;

public class TestTimeProvider implements TimeProvider {
    long time;

    public long getTime() {
        return time;
    }
}
