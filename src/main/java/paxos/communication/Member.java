package paxos.communication;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import javax.validation.constraints.NotNull;

public class Member implements Comparable<Member>, Serializable {
    public static final int DEFAULT_PORT = 2440;
    private final InetAddress address;
    private final int port;
    private final byte[] addressBytes;

    public Member() throws UnknownHostException {
        this(DEFAULT_PORT);
    }

    public Member(int port) throws UnknownHostException {
        this(InetAddress.getLocalHost(), port);
    }

    public Member(InetAddress address, int port) {
        this.address = address;
        this.port = port;
        this.addressBytes = address.getAddress();
    }


    public InetAddress getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public int compareTo(@NotNull Member other) {
        // not using a loop for performance reasons
        if (this.addressBytes[0] < other.addressBytes[0]) return -1;
        if (this.addressBytes[0] > other.addressBytes[0]) return 1;
        if (this.addressBytes[1] < other.addressBytes[1]) return -1;
        if (this.addressBytes[1] > other.addressBytes[1]) return 1;
        if (this.addressBytes[2] < other.addressBytes[2]) return -1;
        if (this.addressBytes[2] > other.addressBytes[2]) return 1;
        if (this.addressBytes[3] < other.addressBytes[3]) return -1;
        if (this.addressBytes[3] > other.addressBytes[3]) return 1;
        return port - other.port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Member member = (Member) o;

        return (port == member.port) && address.equals (member.address);
    }

    @Override
    public int hashCode() {
        int result = address.hashCode();
        result = 31 * result + port;
        return result;
    }

    @Override
    public String toString() {
        return address.getHostName() + ":" + port;
    }
}
