package paxos;

import com.sun.xml.internal.messaging.saaj.soap.Envelope;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.*;
import java.util.List;

/**
 * Implements the unreliable communication.
 */
public class UDPMessenger implements Messenger {
    public static final int BUFFER_SIZE = 128*1024;
    private final List<Member> members;
    private final DatagramSocket socket;
    private final DatagramPacket receivePacket;
    private final Member me;

    public UDPMessenger(List<Member> members, int port) throws SocketException, UnknownHostException {
        this.members = members;
        this.socket = new DatagramSocket(port);
        socket.setReuseAddress(true);
        me = new Member(InetAddress.getLocalHost(), port);
        receivePacket = new DatagramPacket(new byte[BUFFER_SIZE], BUFFER_SIZE, socket.getLocalAddress(), port);
    }

    public Serializable receive() throws IOException, ClassNotFoundException {
        socket.receive(receivePacket);
        if (receivePacket.getLength() > BUFFER_SIZE) throw new IOException("message too big " + receivePacket.getLength());
        Serializable message = (Serializable) PaxosUtils.deserialize(receivePacket.getData());
//        System.out.println(me + " received " + message);
        return message;
    }

    public Member getUID() throws UnknownHostException {
        return me;
    }

    public void sendToAll(Serializable message) {
//        System.out.println(me + " sending " + message + " to all");

        byte[] bytes = PaxosUtils.serialize(message);
        DatagramPacket packet = new DatagramPacket(bytes, bytes.length);
        for (Member member : members) {
            try {
                packet.setAddress(member.getAddress());
                packet.setPort(member.getPort());
                synchronized (this) {
                    socket.send(packet);
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println(me + ": " + e.getMessage());
                // continue to next member
            }
        }
    }

    public void send(Serializable message, Member member) {
//        System.out.println(me + " sending " + message + " to " + member);

        byte[] bytes = PaxosUtils.serialize(message);
        DatagramPacket packet = new DatagramPacket(bytes, bytes.length);
        packet.setAddress(member.getAddress());
        packet.setPort(member.getPort());
        try {
            synchronized (this) {
                socket.send(packet);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println(me + ": " + e.getMessage());
        }
    }

    public int groupSize() {
        return members.size();
    }

    public List<Member> getMembers() {
        return members;
    }

    public void close() {
        socket.close();
    }
}
