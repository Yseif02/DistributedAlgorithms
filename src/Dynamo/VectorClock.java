package Dynamo;

import java.util.*;

/** Vector clock: map nodeId -> counter */
class VectorClock {
    private final Map<String, Integer> clock = new HashMap<>();

    public VectorClock copy() {
        VectorClock vc = new VectorClock();
        vc.clock.putAll(this.clock);
        return vc;
    }

    public void increment(String nodeId) {
        clock.put(nodeId, clock.getOrDefault(nodeId, 0) + 1);
    }

    /** Merge = elementwise max (used when creating a new version from multiple parents). */
    public static VectorClock mergeAll(List<VectorClock> parents) {
        VectorClock merged = new VectorClock();
        for (VectorClock p : parents) {
            for (var e : p.clock.entrySet()) {
                merged.clock.put(e.getKey(), Math.max(merged.clock.getOrDefault(e.getKey(), 0), e.getValue()));
            }
        }
        return merged;
    }

    /**
     * Returns true if this <= other for all entries (treat missing as 0).
     * If so, this is an ancestor of other (can be discarded on read).
     */
    public boolean isAncestorOf(VectorClock other) {
        // For every node in union(this, other): thisCount <= otherCount
        Set<String> all = new HashSet<>(this.clock.keySet());
        all.addAll(other.clock.keySet());
        for (String n : all) {
            int a = this.clock.getOrDefault(n, 0);
            int b = other.clock.getOrDefault(n, 0);
            if (a > b) return false;
        }
        return true;
    }

    @Override public String toString() { return clock.toString(); }
}

class VersionedValue {
    public final String value;          // keep String for simplicity
    public final VectorClock vclock;

    public VersionedValue(String value, VectorClock vc) {
        this.value = value;
        this.vclock = vc;
    }

    @Override public String toString() {
        return "VersionedValue{value=" + value + ", vc=" + vclock + "}";
    }
}

/** What Dynamo returns to the client: the "siblings" plus their clocks (context). */
class GetResult {
    public final List<VersionedValue> siblings; // 1 if syntactic reconciliation possible, >1 if conflict
    public GetResult(List<VersionedValue> siblings) { this.siblings = siblings; }
}
