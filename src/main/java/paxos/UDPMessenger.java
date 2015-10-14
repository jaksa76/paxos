package paxos;

import java.io.IOException;
import java.net.*;
import java.util.List;

public class UDPMessenger implements CommLayer {
    public static final int BUFFER_SIZE = 128*1024;
    private final DatagramSocket socket;
    private final DatagramPacket receivePacket;
    private final ReceivingThread receivingThread;
    private MessageListener listener;
    private boolean running = true;

    public UDPMessenger(int port) throws SocketException, UnknownHostException {
        this.socket = new DatagramSocket(port);
        socket.setReuseAddress(true);
        receivePacket = new DatagramPacket(new byte[BUFFER_SIZE], BUFFER_SIZE, socket.getLocalAddress(), port);
        this.receivingThread = new ReceivingThread();
        this.receivingThread.start();
    }

    public void setListener(MessageListener listener) {
        this.listener = listener;
    }

    public void sendTo(List<Member> members, byte[] message) {
        DatagramPacket packet = new DatagramPacket(message, message.length);
        for (Member member : members) {
            try {
                packet.setAddress(member.getAddress());
                packet.setPort(member.getPort());
                synchronized (this) {
                    socket.send(packet);
                }
            } catch (IOException e) {
                e.printStackTrace();
                // continue to next member
            }
        }
    }

    public void sendTo(Member member, byte[] message) {
        DatagramPacket packet = new DatagramPacket(message, message.length);
        packet.setAddress(member.getAddress());
        packet.setPort(member.getPort());
        try {
            synchronized (this) {
                socket.send(packet);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        this.running = false;
        this.socket.close();
    }

    private class ReceivingThread extends Thread {
        @Override
        public void run() {
            while (running) {
                try {
                    socket.receive(receivePacket);
                    if (receivePacket.getLength() > BUFFER_SIZE)
                        throw new IOException("message too big " + receivePacket.getLength());
                    if (listener != null) listener.receive(receivePacket.getData());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
