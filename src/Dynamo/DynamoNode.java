package Dynamo;

import java.math.BigInteger;
import java.util.*;

class DynamoNode {
    public final String nodeId;
    public final BigInteger token;     // ring position
    public boolean isUp = true;
    public DynamoCluster cluster;

    // Store: key -> list of siblings (concurrent versions)
    private final Map<String, List<VersionedValue>> store = new HashMap<>();

    /**
     * Hinted handoff store: (intendedNodeId, key) -> versions.
     * Slides: if A is down, B stores with metadata “intended recipient”.  [oai_citation:7‡07-Replication (1).pdf](sediment://file_00000000679071fd83f7ca12d8d16b10)
     */
    private final Map<String, List<VersionedValue>> hinted = new HashMap<>();

    public DynamoNode(String nodeId, BigInteger token) {
        this.nodeId = nodeId;
        this.token = token;
    }

    // ----------------------------
    // CLIENT-FACING API (GET/PUT)
    // ----------------------------

    /**
     * Any node can receive get/put.
     * If not in top N of preference list, forward to first among top N.  [oai_citation:8‡07-Replication (1).pdf](sediment://file_00000000679071fd83f7ca12d8d16b10)
     */
    public GetResult clientGet(String key) {
        List<DynamoNode> prefTop = cluster.topNReachable(key);
        DynamoNode coordinator = prefTop.get(0);
        if (coordinator != this) {
            return coordinator.clientGet(key); // “magical network”
        }
        return coordinator.coordinateRead(key);
    }

    /**
     * Client PUT includes optional context: the versions it is updating (siblings from prior read).
     * Slides: client must specify which version it updates via context; subsequent write collapses branches.  [oai_citation:9‡07-Replication (1).pdf](sediment://file_00000000679071fd83f7ca12d8d16b10)
     */
    public void clientPut(String key, String newValue, List<VersionedValue> context /* nullable */) {
        List<DynamoNode> prefTop = cluster.topNReachable(key);
        DynamoNode coordinator = prefTop.get(0);
        if (coordinator != this) {
            coordinator.clientPut(key, newValue, context);
            return;
        }
        coordinator.coordinateWrite(key, newValue, context);
    }

    // ----------------------------
    // COORDINATOR LOGIC (STATE MACHINES)
    // ----------------------------

    /**
     * Read state machine:
     * - send read to N nodes
     * - wait for R responses
     * - return causally-unrelated leaves (siblings) and do read repair.
     */
    private GetResult coordinateRead(String key) {
        List<DynamoNode> replicas = cluster.topNReachable(key); // N reachable
        Map<DynamoNode, List<VersionedValue>> replies = new HashMap<>();

        // 1) send read requests (in real Dynamo: async + timeout)
        for (DynamoNode n : replicas) {
            replies.put(n, n.onReplicaRead(key));
        }

        // 2) wait for R replies (here: assume immediate; if <R, fail)
        if (replies.size() < cluster.R) {
            throw new RuntimeException("Read failed: not enough replicas reachable");
        }

        // 3) merge all versions we got
        List<VersionedValue> all = new ArrayList<>();
        for (List<VersionedValue> vv : replies.values()) all.addAll(vv);

        // 4) compute “leaves”: remove any version that is an ancestor of another
        List<VersionedValue> leaves = pruneAncestors(all);

        // 5) read repair: if a replica returned stale versions, push the latest back.
        for (var e : replies.entrySet()) {
            DynamoNode replica = e.getKey();
            List<VersionedValue> replicaVersions = e.getValue();
            if (isStale(replicaVersions, leaves)) {
                replica.onReplicaWriteRepair(key, leaves);
            }
        }

        // If leaves.size() == 1, caller can treat it as syntactically reconciled.
        // If >1, this is a conflict: return all siblings (application may reconcile).
        return new GetResult(leaves);
    }

    /**
     * Write state machine:
     * - generate vector clock for new version
     * - write locally
     * - send to N highest-ranked reachable nodes
     * - succeed after W acks.
     */
    private void coordinateWrite(String key, String newValue, List<VersionedValue> context /* nullable */) {
        List<DynamoNode> replicas = cluster.topNReachable(key);

        // 1) Determine parent clocks from context, merge, then increment my entry
        List<VectorClock> parents = new ArrayList<>();
        if (context != null) {
            for (VersionedValue vv : context) parents.add(vv.vClock());
        } else {
            // If no context, you’re creating a new branch from whatever you have locally
            for (VersionedValue vv : getLocal(key)) parents.add(vv.vClock());
        }

        VectorClock merged = VectorClock.mergeAll(parents);
        merged.increment(this.nodeId);
        VersionedValue newVer = new VersionedValue(newValue, merged);

        // 2) Write locally (this node is coordinator; “always writeable” behavior)
        putLocal(key, newVer);

        // 3) Send to replicas (skip down nodes; if intended node is down, hinted handoff)
        int acks = 1; // local write ack
        for (DynamoNode r : replicas) {
            if (r == this) continue;
            boolean ok = sendReplicaPut(r, key, newVer);
            if (ok) acks++;
            if (acks >= cluster.W) break;
        }

        if (acks < cluster.W) {
            // Dynamo can still choose to return success depending on configuration;
            // here we mimic the spec: need W acknowledgements.
            throw new RuntimeException("Write failed: not enough replicas acked");
        }
    }

    // ----------------------------
    // “NETWORK” SEND/RECEIVE (REPLICA MESSAGES)
    // ----------------------------

    private boolean sendReplicaPut(DynamoNode target, String key, VersionedValue v) {
        if (!target.isUp) {
            // hinted handoff: store on *this* node tagged for target
            String hintKey = target.nodeId + "|" + key;
            hinted.computeIfAbsent(hintKey, k -> new ArrayList<>()).add(v);
            return false;
        }
        target.onReplicaPut(key, v);
        return true;
    }

    /** Replica handler for PUT from coordinator. */
    private void onReplicaPut(String key, VersionedValue v) {
        putLocal(key, v);
    }

    /** Replica handler for READ from coordinator. */
    private List<VersionedValue> onReplicaRead(String key) {
        return new ArrayList<>(getLocal(key));
    }

    /** Read repair handler: coordinator pushes latest leaf set to stale replica. */
    private void onReplicaWriteRepair(String key, List<VersionedValue> latestLeaves) {
        // Simplest repair: replace local versions with latest leaves
        store.put(key, new ArrayList<>(latestLeaves));
    }

    /**
     * Periodic background: try deliver hinted handoff replicas if intended node recovered.
     * Slides: once transfer succeeds, delete local hint without reducing replica count.
     */
    public void runHintedHandoffTick() {
        if (hinted.isEmpty()) return;

        Iterator<Map.Entry<String, List<VersionedValue>>> it = hinted.entrySet().iterator();
        while (it.hasNext()) {
            var e = it.next();
            String[] parts = e.getKey().split("\\|", 2);
            String intendedNodeId = parts[0];
            String key = parts[1];

            DynamoNode intended = findNode(intendedNodeId);
            if (intended != null && intended.isUp) {
                for (VersionedValue v : e.getValue()) {
                    intended.onReplicaPut(key, v);
                }
                it.remove(); // delete hint after successful transfer
            }
        }
    }

    private DynamoNode findNode(String id) {
        for (DynamoNode n : cluster.ring) if (n.nodeId.equals(id)) return n;
        return null;
    }

    // ----------------------------
    // LOCAL STORAGE + VERSION PRUNING
    // ----------------------------

    private List<VersionedValue> getLocal(String key) {
        return store.getOrDefault(key, new ArrayList<>());
    }

    private void putLocal(String key, VersionedValue newVer) {
        // Dynamo keeps multiple versions; but most new versions subsume old ones.
        // Keep all versions that are NOT ancestors of the new one; also if new one
        // is an ancestor of an existing one, keep existing (it dominates new).
        List<VersionedValue> cur = new ArrayList<>(getLocal(key));

        // If any existing dominates new, then new is redundant (don’t store it)
        for (VersionedValue existing : cur) {
            if (newVer.vClock().isAncestorOf(existing.vClock())) {
                store.put(key, cur);
                return;
            }
        }

        // Remove versions that are ancestors of new
        List<VersionedValue> next = new ArrayList<>();
        for (VersionedValue existing : cur) {
            if (!existing.vClock().isAncestorOf(newVer.vClock())) next.add(existing);
        }
        next.add(newVer);
        store.put(key, next);
    }

    /** Leaves = versions that are not ancestors of any other version. */
    private static List<VersionedValue> pruneAncestors(List<VersionedValue> all) {
        List<VersionedValue> leaves = new ArrayList<>();
        for (int i = 0; i < all.size(); i++) {
            VersionedValue a = all.get(i);
            boolean isAncestor = false;
            for (int j = 0; j < all.size(); j++) {
                if (i == j) continue;
                VersionedValue b = all.get(j);
                if (a.vClock().isAncestorOf(b.vClock()) && !b.vClock().isAncestorOf(a.vClock())) {
                    isAncestor = true;
                    break;
                }
            }
            if (!isAncestor) leaves.add(a);
        }
        return dedupe(leaves);
    }

    private static boolean isStale(List<VersionedValue> replica, List<VersionedValue> leaves) {
        // Stale if replica is missing any leaf or has only ancestors of leaves.
        // Simple heuristic: if replica set exactly equals leaves -> not stale.
        return !sameSet(replica, leaves);
    }

    private static boolean sameSet(List<VersionedValue> a, List<VersionedValue> b) {
        if (a.size() != b.size()) return false;
        // Compare by (value + vclock string) for simplicity
        Set<String> A = new HashSet<>();
        Set<String> B = new HashSet<>();
        for (var x : a) A.add(x.value() + "|" + x.vClock().toString());
        for (var x : b) B.add(x.value() + "|" + x.vClock().toString());
        return A.equals(B);
    }

    private static List<VersionedValue> dedupe(List<VersionedValue> xs) {
        Map<String, VersionedValue> m = new LinkedHashMap<>();
        for (var x : xs) m.put(x.value() + "|" + x.vClock().toString(), x);
        return new ArrayList<>(m.values());
    }

    @Override public String toString() { return "Node(" + nodeId + ")"; }
}
