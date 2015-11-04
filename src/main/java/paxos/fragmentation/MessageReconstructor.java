package paxos.fragmentation;

import paxos.communication.CommLayer;
import paxos.communication.UDPMessenger;

import java.util.HashMap;
import java.util.Map;

/**
 * Collects fragments of messages and recomposes them.
 */
public class MessageReconstructor {
    private final Map<Long, FragmentCollector> collectors = new HashMap<Long, FragmentCollector>();

    /**
     * @param messageFragment
     *
     * @return the complete message if we have all the parts or null otherwise
     */
    byte[] collectFragment(MessageFragment messageFragment) {
        FragmentCollector collector = getOrCreateCollector(messageFragment);
        collector.addPart(messageFragment.fragmentNo, messageFragment.part);

        if (collector.isComplete()) {
            collectors.remove(messageFragment.id);
            return collector.extractMessage();
        } else {
            return null;
        }
    }

    private FragmentCollector getOrCreateCollector(MessageFragment messageFragment) {
        if (!collectors.containsKey(messageFragment.id)) {
            collectors.put(messageFragment.id, new FragmentCollector(messageFragment.totalFragments));
        }
        return collectors.get(messageFragment.id);
    }
}
