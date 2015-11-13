package paxos;

import paxos.communication.Member;

import java.io.Serializable;
import java.util.*;

public class PropertyBasedTesting {
    private static class Oracle {
        private final double seed;
        private long n;

        public Oracle(double seed) {
            this.seed = seed;
        }

        public double nextRand() {
            return hash(seed, n++);
        }

        private double hash(double x, float y) {
            double dot = x*4567.4567+y*6789.8756;
            return Math.sin(dot*123.321)*321.123 % 1.0;
        }
    }

    private static class System {
        private final List<BasicGroup> groups;
        private final LinkedHashMap<Serializable, Member> messageDestinations = new LinkedHashMap<Serializable, Member>();
        private final List<Serializable> travellingMessages = new ArrayList<Serializable>();
        private final boolean membersAlive[];
        private final Oracle oracle;

        private System(List<BasicGroup> groups, double seed) {
            this.groups = groups;
            this.membersAlive = new boolean[groups.size()];
            this.oracle = new Oracle(seed);
        }

        public void makeAStep() {
            double rnd = oracle.nextRand();
            if (rnd < .3) deliverAMessage();
            else if (rnd < .6) advanceTimeSomewhere();
            else killAMember();
        }

        private void advanceTimeSomewhere() {
            int rnd = (int) oracle.nextRand() * groups.size();
            long delta = (long) (oracle.nextRand() * 1000);
            if (membersAlive[rnd]) advanceTime(groups.get(rnd), delta);
        }

        private void deliverAMessage() {
            int rnd = (int) oracle.nextRand() * messageDestinations.size();
            deliverMessage(travellingMessages.get(rnd));
        }

        public void deliverMessage(Serializable msg) {
            // TODO remove message from messageDestinations and travellingMessages
            // TODO get destination and check if it is alive
            // TODO process message
            // TODO add new messages to messageDestinations and travellingMEssages
        }

        public void killAMember() {
            int rnd = (int) oracle.nextRand() * groups.size();
            membersAlive[rnd] = false;
        }

        public void advanceTime(BasicGroup member, long time) {
            // TODO process time advancement
            // TODO add new messages to messageDestinations and travellingMEssages
        }
    }
}
