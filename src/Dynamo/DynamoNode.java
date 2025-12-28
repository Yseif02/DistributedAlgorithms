package Dynamo;

import java.math.BigInteger;
import java.util.*;

/**
 * DynamoNode
 *
 * Represents a single Dynamo storage node.
 *
 * A node can act as:
 *  - coordinator (for some keys)
 *  - replica (for keys coordinated by others)
 *
 * Dynamo is *leaderless*:
 *  - any node can accept reads/writes
 *  - requests are forwarded to the key’s coordinator
 */
class DynamoNode {

    /* =========================================================
     * Identity and cluster membership
     * ========================================================= */

    public final String nodeId;
    public final BigInteger token;   // position on the consistent hash ring

    public boolean isUp = true;
    // Used to simulate failures

    public DynamoCluster cluster;
    // Set by DynamoCluster constructor

    /* =========================================================
     * Local storage
     * ========================================================= */

    /**
     * Main key-value store.
     *
     * Key → list of versions (siblings).
     *
     * Dynamo allows multiple concurrent versions
     * instead of forcing a total order.
     */
    private final Map<String, List<VersionedValue>> store = new HashMap<>();

    /**
     * Hinted handoff storage.
     *
     * If a replica is down, another node temporarily
     * stores its data along with metadata indicating
     * the intended recipient.
     */
    private final Map<String, List<VersionedValue>> hinted = new HashMap<>();

    public DynamoNode(String nodeId, BigInteger token) {
        this.nodeId = nodeId;
        this.token = token;
    }

    /* =========================================================
     * CLIENT-FACING API
     * ========================================================= */

    /**
     * Client GET operation.
     *
     * Any node can receive the request.
     * If this node is not the coordinator, forward
     * to the coordinator.
     */
    public GetResult clientGet(String key) {
        List<DynamoNode> prefTop = cluster.topNReachable(key);
        DynamoNode coordinator = prefTop.get(0);

        if (coordinator != this) {
            return coordinator.clientGet(key);
        }
        return coordinateRead(key);
    }

    /**
     * Client PUT operation.
     *
     * The client optionally supplies context (vector clocks)
     * from a previous read.
     *
     * Providing context allows the system to:
     *  - collapse branches
     *  - avoid unnecessary conflicts
     */
    public void clientPut(String key, String newValue, List<VersionedValue> context) {
        List<DynamoNode> prefTop = cluster.topNReachable(key);
        DynamoNode coordinator = prefTop.get(0);

        if (coordinator != this) {
            coordinator.clientPut(key, newValue, context);
            return;
        }
        coordinateWrite(key, newValue, context);
    }

    /* =========================================================
     * COORDINATOR READ PATH
     * ========================================================= */

    /**
     * Implements Dynamo’s read state machine.
     *
     * Steps:
     *  1. Send read requests to N replicas
     *  2. Wait for R responses
     *  3. Merge versions
     *  4. Remove ancestor versions
     *  5. Perform read repair if needed
     */
    private GetResult coordinateRead(String key) {
        List<DynamoNode> replicas = cluster.topNReachable(key);
        Map<DynamoNode, List<VersionedValue>> replies = new HashMap<>();

        // Send read requests
        for (DynamoNode n : replicas) {
            replies.put(n, n.onReplicaRead(key));
        }

        if (replies.size() < cluster.R) {
            throw new RuntimeException("Read failed: insufficient replicas");
        }

        // Merge all returned versions
        List<VersionedValue> all = new ArrayList<>();
        for (List<VersionedValue> vv : replies.values()) {
            all.addAll(vv);
        }

        // Keep only leaf versions
        List<VersionedValue> leaves = pruneAncestors(all);

        // Read repair: update stale replicas
        for (var e : replies.entrySet()) {
            if (isStale(e.getValue(), leaves)) {
                e.getKey().onReplicaWriteRepair(key, leaves);
            }
        }

        return new GetResult(leaves);
    }

    /* =========================================================
     * COORDINATOR WRITE PATH
     * ========================================================= */

    /**
     * Implements Dynamo’s write state machine.
     *
     * Steps:
     *  1. Merge parent vector clocks
     *  2. Increment coordinator’s clock
     *  3. Write locally
     *  4. Replicate to N replicas
     *  5. Return success after W acknowledgements
     */
    private void coordinateWrite(String key, String newValue, List<VersionedValue> context) {
        List<DynamoNode> replicas = cluster.topNReachable(key);

        List<VectorClock> parents = new ArrayList<>();
        if (context != null) {
            for (VersionedValue vv : context) parents.add(vv.vClock());
        } else {
            for (VersionedValue vv : getLocal(key)) parents.add(vv.vClock());
        }

        VectorClock merged = VectorClock.mergeAll(parents);
        merged.increment(this.nodeId);
        VersionedValue newVer = new VersionedValue(newValue, merged);

        putLocal(key, newVer);

        int acks = 1;
        for (DynamoNode r : replicas) {
            if (r == this) continue;
            if (sendReplicaPut(r, key, newVer)) acks++;
            if (acks >= cluster.W) break;
        }

        if (acks < cluster.W) {
            throw new RuntimeException("Write failed: insufficient acknowledgements");
        }
    }

    /* =========================================================
     * REPLICA HANDLERS
     * ========================================================= */

    private boolean sendReplicaPut(DynamoNode target, String key, VersionedValue v) {
        if (!target.isUp) {
            String hintKey = target.nodeId + "|" + key;
            hinted.computeIfAbsent(hintKey, k -> new ArrayList<>()).add(v);
            return false;
        }
        target.onReplicaPut(key, v);
        return true;
    }

    private void onReplicaPut(String key, VersionedValue v) {
        putLocal(key, v);
    }

    private List<VersionedValue> onReplicaRead(String key) {
        return new ArrayList<>(getLocal(key));
    }

    private void onReplicaWriteRepair(String key, List<VersionedValue> latestLeaves) {
        store.put(key, new ArrayList<>(latestLeaves));
    }

    /* =========================================================
     * HINTED HANDOFF
     * ========================================================= */

    /**
     * Periodically attempts to deliver hinted data
     * back to recovered replicas.
     */
    public void runHintedHandoffTick() {
        Iterator<Map.Entry<String, List<VersionedValue>>> it = hinted.entrySet().iterator();

        while (it.hasNext()) {
            var e = it.next();
            String[] parts = e.getKey().split("\\|", 2);
            DynamoNode intended = findNode(parts[0]);

            if (intended != null && intended.isUp) {
                for (VersionedValue v : e.getValue()) {
                    intended.onReplicaPut(parts[1], v);
                }
                it.remove();
            }
        }
    }

    private DynamoNode findNode(String id) {
        for (DynamoNode n : cluster.ring)
            if (n.nodeId.equals(id)) return n;
        return null;
    }

    /* =========================================================
     * LOCAL VERSION MANAGEMENT
     * ========================================================= */

    private List<VersionedValue> getLocal(String key) {
        return store.getOrDefault(key, new ArrayList<>());
    }

    /**
     * Inserts a new version while enforcing
     * Dynamo’s version-pruning rules.
     */
    private void putLocal(String key, VersionedValue newVer) {
        List<VersionedValue> cur = new ArrayList<>(getLocal(key));

        for (VersionedValue existing : cur) {
            if (newVer.vClock().isAncestorOf(existing.vClock())) {
                store.put(key, cur);
                return;
            }
        }

        List<VersionedValue> next = new ArrayList<>();
        for (VersionedValue existing : cur) {
            if (!existing.vClock().isAncestorOf(newVer.vClock()))
                next.add(existing);
        }
        next.add(newVer);
        store.put(key, next);
    }

    /* =========================================================
     * VERSION COMPARISON HELPERS
     * ========================================================= */

    private static List<VersionedValue> pruneAncestors(List<VersionedValue> all) {
        List<VersionedValue> leaves = new ArrayList<>();

        for (VersionedValue a : all) {
            boolean ancestor = false;
            for (VersionedValue b : all) {
                if (a != b && a.vClock().isAncestorOf(b.vClock())) {
                    ancestor = true;
                    break;
                }
            }
            if (!ancestor) leaves.add(a);
        }
        return leaves;
    }

    private static boolean isStale(List<VersionedValue> replica, List<VersionedValue> leaves) {
        return !sameSet(replica, leaves);
    }

    private static boolean sameSet(List<VersionedValue> a, List<VersionedValue> b) {
        if (a.size() != b.size()) return false;
        Set<String> A = new HashSet<>();
        Set<String> B = new HashSet<>();
        for (var x : a) A.add(x.value() + "|" + x.vClock());
        for (var x : b) B.add(x.value() + "|" + x.vClock());
        return A.equals(B);
    }

    @Override
    public String toString() {
        return "Node(" + nodeId + ")";
    }
}