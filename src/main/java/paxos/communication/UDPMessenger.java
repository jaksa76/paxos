package paxos.communication;

import paxos.PaxosUtils;

import java.io.IOException;
import java.net.*;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class UDPMessenger implements CommLayer {
    public static final int BUFFER_SIZE = 128*1024;
    public static final int UPDATE_PERIOD = 100;
    private final DatagramSocket socket;
    private final DatagramPacket receivePacket;
    private final ReceivingThread receivingThread;
    private final TickingThread tickingThread;
    private final DispatchingThread dispatchThread;
    private MessageListener listener;
    private boolean running = true;
    private BlockingQueue<byte[]> msgQueue = new LinkedBlockingQueue<byte[]>();

    public UDPMessenger() throws SocketException, UnknownHostException {
        this(2440);
    }

    public UDPMessenger(int port) throws SocketException, UnknownHostException {
        this.socket = new DatagramSocket(port);
        socket.setReuseAddress(true);
        receivePacket = new DatagramPacket(new byte[BUFFER_SIZE], BUFFER_SIZE, socket.getLocalAddress(), port);
        this.receivingThread = new ReceivingThread();
        this.tickingThread = new TickingThread();
        this.dispatchThread = new DispatchingThread();
        this.receivingThread.start();
        this.tickingThread.start();
        this.dispatchThread.start();
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
                if (running) e.printStackTrace();
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
            if (running) e.printStackTrace();
        }
    }

    public void close() {
        this.running = false;
        this.socket.close();
        this.dispatchThread.interrupt();
    }

    private class ReceivingThread extends Thread {
        @Override
        public void run() {
            while (running) {
                try {
                    socket.receive(receivePacket);
//                    System.out.println("received message");
                    if (receivePacket.getLength() > BUFFER_SIZE)
                        throw new IOException("message too big " + receivePacket.getLength());
                    msgQueue.put(receivePacket.getData().clone());
                } catch (IOException e) {
                    if (running) e.printStackTrace();
                } catch (InterruptedException e) {
                    if (running) e.printStackTrace();
                }
            }
        }
    }

    private class DispatchingThread extends Thread {
        @Override
        public void run() {
            try {
                while (running) {
                    byte[] msg = msgQueue.take();
                    if (running) dispatch(msg);
                }
            } catch (InterruptedException e) {
                if (running) e.printStackTrace();
            }
        }
    }

    private class TickingThread extends Thread {
        @Override
        public void run() {
            try {
                while (running) {
                    dispatch(PaxosUtils.serialize(new Tick(System.currentTimeMillis())));
                    sleep(UPDATE_PERIOD);
                }
            } catch (Exception e) {
                if (running) e.printStackTrace();
            }
        }
    };

    private synchronized void dispatch(byte[] msg) {
        if (listener != null) listener.receive(msg);
    }

}
