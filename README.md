# Paxos
A Java implementation of Lamport's Paxos algorithm.

## Why another Paxos implementation?

This is an implementation of totally ordered atomic broadcast protocol. The algorithm used is a variant of Lamport's Paxos.
This library is a lightweight alternative to Apache Zookeper. Zookeper uses Paxos to keep consistency among the replicas, 
but the clients are remote. This poses several limits to how Zookeeper can be used.
Our library is used from within the VM and clients can use it to build replication with strong consistency guarantees 
regardless of how state is represented.

## Why a Variant of Paxos?

The original algorithm requires state to be persisted. This significantly reduces performance, but it is required for enabling 
members to recover. Our solution does not support recovery of members, but instead supports changing the members of the group. 
This allows us to avoid persistence and have a much higher throughput.

# How to use it?

This is the basic group implementation:

'''java
        // this is the list of members
        Members members = new Members(
                new Member(), // this is a reference to a member on the localhost on default port (2440)
                new Member(2441), // this one is on localhost with the specified port
                new Member(InetAddress.getLocalHost(), 2442)); // you can specify the address and port manually

        // we need to define a receiver
        class MyReceiver implements Receiver {
            // we follow a reactive pattern here
            public void receive(Serializable message) {
                System.out.println("received " + message.toString());
            }
        };

        // this actually creates the members
        BasicGroup group1 = new BasicGroup(members.get(0), new MyReceiver());
        BasicGroup group2 = new BasicGroup(members.get(1), new MyReceiver());
        BasicGroup group3 = new BasicGroup(members.get(2), new MyReceiver());

        // this will cause all receivers to print "received Hello"
        group2.broadcast("Hello");

        Thread.sleep(1); // allow the members to receive the message

        group1.close(); group2.close(); group3.close();
'''
