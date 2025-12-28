package Dynamo;

import java.security.MessageDigest;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

class DynamoCluster {
    public final List<DynamoNode> ring; // sorted by token ascending
    public final int N, R, W;

    public DynamoCluster(List<DynamoNode> nodes, int N, int R, int W) {
        this.N = N; this.R = R; this.W = W;
        nodes.sort(Comparator.comparing(n -> n.token));
        this.ring = nodes;
        for (DynamoNode n : ring) n.cluster = this;
    }

    /** MD5(key) -> 128-bit token on the ring. */
    public static BigInteger hashToToken(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] dig = md.digest(key.getBytes());
            return new BigInteger(1, dig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** First node clockwise at/after token. */
    public DynamoNode coordinatorForKey(String key) {
        BigInteger t = hashToToken(key);
        for (DynamoNode n : ring) {
            if (n.token.compareTo(t) >= 0) return n;
        }
        return ring.get(0); // wraparound
    }

    /**
     * Preference list (length >= N in real Dynamo); here we build a simple list
     * of ring order starting from coordinator.
     */
    public List<DynamoNode> preferenceList(String key) {
        DynamoNode start = coordinatorForKey(key);
        int idx = ring.indexOf(start);

        List<DynamoNode> pref = new ArrayList<>();
        for (int i = 0; i < ring.size(); i++) {
            pref.add(ring.get((idx + i) % ring.size()));
        }
        return pref;
    }

    /**
     * The "top N healthy nodes" from the preference list.
     * Slides: coordinator sends to first N healthy nodes, skipping down ones.
     */
    public List<DynamoNode> topNReachable(String key) {
        List<DynamoNode> pref = preferenceList(key);
        List<DynamoNode> chosen = new ArrayList<>();
        for (DynamoNode n : pref) {
            if (n.isUp) chosen.add(n);
            if (chosen.size() == N) break;
        }
        return chosen;
    }
}
