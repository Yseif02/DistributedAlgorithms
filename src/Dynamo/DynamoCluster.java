package Dynamo;

import java.security.MessageDigest;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * DynamoCluster
 *
 * Represents the global cluster view used by Dynamo:
 *  - consistent hashing ring
 *  - replication parameters (N, R, W)
 *
 * This class is NOT a node.
 * It exists to:
 *  - compute key placement on the ring
 *  - determine coordinators
 *  - build preference lists
 *
 * Think of this as the "routing / metadata layer".
 */
class DynamoCluster {

    /* =========================================================
     * Ring and quorum parameters
     * ========================================================= */

    public final List<DynamoNode> ring;
    // Nodes sorted by hash token (clockwise ring)

    public final int N;
    // Replication factor: number of replicas per key

    public final int R;
    // Read quorum size

    public final int W;
    // Write quorum size

    /**
     * Constructs the cluster and initializes the ring.
     *
     * @param nodes all Dynamo nodes
     * @param N replication factor
     * @param R read quorum
     * @param W write quorum
     */
    public DynamoCluster(List<DynamoNode> nodes, int N, int R, int W) {
        this.N = N;
        this.R = R;
        this.W = W;

        // Sort nodes by token to form the consistent hashing ring
        nodes.sort(Comparator.comparing(n -> n.token));
        this.ring = nodes;

        // Give each node a pointer back to this cluster
        for (DynamoNode n : ring) {
            n.cluster = this;
        }
    }

    /* =========================================================
     * Consistent hashing utilities
     * ========================================================= */

    /**
     * Hashes a key into a 128-bit token using MD5.
     *
     * In real Dynamo:
     *  - tokens are spread evenly
     *  - virtual nodes are used
     *
     * Here we keep it simple.
     */
    public static BigInteger hashToToken(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] dig = md.digest(key.getBytes());
            return new BigInteger(1, dig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the coordinator node for a key.
     *
     * Rule:
     *  - first node clockwise whose token >= key hash
     *  - wrap around if needed
     */
    public DynamoNode coordinatorForKey(String key) {
        BigInteger t = hashToToken(key);
        for (DynamoNode n : ring) {
            if (n.token.compareTo(t) >= 0) {
                return n;
            }
        }
        return ring.get(0); // wraparound
    }

    /* =========================================================
     * Preference lists and replica selection
     * ========================================================= */

    /**
     * Builds the preference list for a key.
     *
     * This is the ordered list of nodes clockwise from
     * the coordinator.
     *
     * In real Dynamo:
     *  - preference list length > N
     *  - used to skip failed nodes
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
     * Returns the top N *reachable* nodes from the preference list.
     *
     * This is critical to Dynamo’s availability:
     *  - failed nodes are skipped
     *  - hinted handoff is used if needed
     */
    public List<DynamoNode> topNReachable(String key) {
        List<DynamoNode> pref = preferenceList(key);
        List<DynamoNode> chosen = new ArrayList<>();

        for (DynamoNode n : pref) {
            if (n.isUp) {
                chosen.add(n);
            }
            if (chosen.size() == N) break;
        }
        return chosen;
    }
}