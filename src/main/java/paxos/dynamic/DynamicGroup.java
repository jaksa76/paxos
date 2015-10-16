package paxos.dynamic;

import paxos.*;
import paxos.fragmentation.FragmentingGroup;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    void onNewJoin(Member joiner) {
        final FragmentingGroup group = groups.get(groups.size() - 1);
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

    private long createGroupUID() {
        return (long) (Math.random()*Long.MAX_VALUE);
    }

    public void broadcast(Serializable message) throws IOException {
        groups.get(groups.size()-1).broadcast(message);
    }

    public void receive(Serializable message) {
        if (message instanceof GroupChange) {
            // TODO stop receiving messages from previous groups
            GroupChange groupChangeMessage = (GroupChange) message;

            GroupMembership membership = new GroupMembership(groupChangeMessage.members, me);
            FilteringMessenger filteringMessenger = new FilteringMessenger(groupChangeMessage.groupId, commLayer, multiListener);
            this.groups.add(new FragmentingGroup(membership, filteringMessenger, this));
        } else {
            receiver.receive(message);
        }
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
            } else if (message instanceof JoinRequest) {
                JoinRequest joinRequest = (JoinRequest) message;
                onNewJoin(joinRequest.joiner);
            } else {
                throw new RuntimeException("Unsupported message type: " + message.getClass().getName());
            }
        }
    }
}
