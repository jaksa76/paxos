package paxos.dynamic;

import paxos.communication.CommLayer;

import java.util.ArrayList;

class MultiListener implements CommLayer.MessageListener {
    private ArrayList<CommLayer.MessageListener> listeners = new ArrayList<CommLayer.MessageListener>();

    public void addListener(CommLayer.MessageListener listener) {
        listeners = new ArrayList<CommLayer.MessageListener>(listeners);
        listeners.add(listener);
    }

    public void receive(byte[] message) {
        for (CommLayer.MessageListener listener : listeners) {
            listener.receive(message);
        }
    }
}
