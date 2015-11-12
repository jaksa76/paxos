package paxos.dynamic;

import paxos.*;
import paxos.communication.CommLayer;
import paxos.communication.Member;
import paxos.communication.Tick;
import paxos.communication.UDPMessenger;
import paxos.fragmentation.FragmentingGroup;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This class implements totally ordered reliable broadcast and it is also able to dynamically change
 * the members of the group as long as the majority of the members is reachable.
 */
public class DynamicGroup implements Receiver {
    public static final int BUFFER_SIZE = 128*1024;

    // we should not terminate a group after we receive a GroupChage message, se we keep the list of all groups ever created.
    // TODO we might want to limit the size of this list
    private final List<FragmentingGroup> groups = new ArrayList<FragmentingGroup>();

    private final Member me;
    private final CommLayer commLayer;
    private final Receiver receiver;
    private List<Member> members;
    private MultiListener multiListener = new MultiListener();

    public DynamicGroup(int port, Receiver receiver) throws IOException, InterruptedException {
        this(new UDPMessenger(), receiver, port, Collections.<Member>emptyList());
    }

    public DynamicGroup(Member knownMember, Receiver receiver) throws IOException, InterruptedException {
        this(new UDPMessenger(), receiver, 2440, Collections.singletonList(knownMember));
    }

    public DynamicGroup(int port, Member knownMember, Receiver receiver) throws IOException, InterruptedException {
        this(new UDPMessenger(port), receiver, port, Collections.singletonList(knownMember));
    }

    public DynamicGroup(int port, List<Member> knownMembers, Receiver receiver) throws IOException, InterruptedException {
        this(new UDPMessenger(port), receiver, port, knownMembers);
    }

    public DynamicGroup(CommLayer commLayer, Receiver receiver, int port, List<Member> knownMembers) throws IOException, InterruptedException {
        this.commLayer = commLayer;
        this.receiver = receiver;
        this.me = new Member(InetAddress.getLocalHost(), port);
        GroupInfo groupInfo = requestGroupInfo(knownMembers);
        this.members = groupInfo.members;

        GroupMembership membership = new GroupMembership(groupInfo.members, me);
        FilteringMessenger filteringMessenger = new FilteringMessenger(groupInfo.groupId, commLayer, multiListener);
        FragmentingGroup fragmentingGroup = new FragmentingGroup(membership, filteringMessenger, this);
        this.groups.add(fragmentingGroup);
    }

    private GroupInfo requestGroupInfo(final List<Member> knownMembers) throws InterruptedException {
        if (knownMembers.isEmpty()) {
            List<Member> members = new ArrayList<Member>();
            members.add(me);
            return new GroupInfo(members, 0);
        }
        for (Member knownMember : knownMembers) {
            GroupInfo info = requestGroupInfo(knownMember);
            if (info != null) return info;
        }
        throw new RuntimeException("Could not get group info from any of the members " + knownMembers);
    }

    private GroupInfo requestGroupInfo(Member member) throws InterruptedException {
        final Serializable[] messageHolder = new Serializable[1];
        commLayer.setListener(new CommLayer.MessageListener() {
            public void receive(byte[] bytes) {
                Serializable message = (Serializable) PaxosUtils.deserialize(bytes);
                if (message instanceof GroupInfo) messageHolder[0] = message;
            }
        });

        commLayer.sendTo(member, PaxosUtils.serialize(new JoinRequest(me)));

        Thread.sleep(1000);
        return (GroupInfo) messageHolder[0];
    }

    public void addMember(Member joiner) {
        final FragmentingGroup group = getCurrentGroup();
        members = new ArrayList<Member>(members);
        members.add(joiner);
        Collections.sort(members);
        final long groupUID = createGroupUID();
        new Thread() {
            @Override
            public void run() {
                try {
                    group.broadcast(new GroupChange(groupUID, members));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        // send GroupInfo to joiner
        commLayer.sendTo(joiner, PaxosUtils.serialize(new GroupInfo(members, groupUID)));
    }

    private FragmentingGroup getCurrentGroup() {
        return groups.get(groups.size() - 1);
    }

    public void removeMember(Member leaver) {
        final FragmentingGroup group = getCurrentGroup();
        members = new ArrayList<Member>(members);
        members.remove(leaver);
        Collections.sort(members);
        final long groupUID = createGroupUID();
        new Thread() {
            @Override
            public void run() {
                try {
                    group.broadcast(new GroupChange(groupUID, members));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }

    private long createGroupUID() {
        return (long) (Math.random()*Long.MAX_VALUE);
    }

    public void broadcast(Serializable message) throws IOException {
        getCurrentGroup().broadcast(message);
    }

    public void receive(Serializable message) {
        if (message instanceof GroupChange) {
            GroupChange groupChangeMessage = (GroupChange) message;

            if (groupChangeMessage.members.contains(me)) {
                GroupMembership membership = new GroupMembership(groupChangeMessage.members, me);
                FilteringMessenger filteringMessenger = new FilteringMessenger(groupChangeMessage.groupId, commLayer, multiListener);
                this.groups.add(new FragmentingGroup(membership, filteringMessenger, this));
            } else {
                close();
            }
        } else {
            receiver.receive(message);
        }
    }

    public void close() {
        for (FragmentingGroup group : groups) {
            group.close();
        }
        commLayer.close();
    }

    public class FilteringMessenger implements CommLayer, CommLayer.MessageListener {
        private final long groupID;
        private final CommLayer commLayer;
        private MessageListener listener;

        public FilteringMessenger(long groupID, CommLayer commLayer, MultiListener multiListener) {
            this.groupID = groupID;
            this.commLayer = commLayer;
            this.commLayer.setListener(multiListener);
            multiListener.addListener(this);
        }

        public void setListener(MessageListener listener) {
            this.listener = listener;
        }

        public void sendTo(List<Member> members, byte[] message) {
            commLayer.sendTo(members, PaxosUtils.serialize(new DynamicGroupMessage(groupID, message)));
        }

        public void sendTo(Member member, byte[] message) {
            commLayer.sendTo(member, PaxosUtils.serialize(new DynamicGroupMessage(groupID, message)));
        }

        public void close() {
            commLayer.close();
        }

        public void receive(byte[] bytes) {
            Serializable message = (Serializable) PaxosUtils.deserialize(bytes);
            if (message instanceof DynamicGroupMessage) {
                DynamicGroupMessage dynamicGroupMessage = (DynamicGroupMessage) message;
                if (dynamicGroupMessage.groupId == this.groupID) {
                    listener.receive(dynamicGroupMessage.message);
                }
            } else if (message instanceof Tick) {
                listener.receive(bytes);
            } else if (message instanceof JoinRequest) {
                JoinRequest joinRequest = (JoinRequest) message;
                addMember(joinRequest.joiner);
            } else {
                throw new RuntimeException("Unsupported message type: " + message.getClass().getName());
            }
        }
    }
}
