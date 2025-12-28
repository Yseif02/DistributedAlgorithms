package Dynamo;

import java.util.*;

/**
 * VectorClock
 *
 * Tracks causality between versions.
 *
 * Each entry:
 *   nodeId → counter
 *
 * Used by Dynamo to:
 *  - detect concurrent writes
 *  - prune obsolete versions
 */
class VectorClock {

    private final Map<String, Integer> clock = new HashMap<>();

    /** Creates a deep copy of this vector clock. */
    public VectorClock copy() {
        VectorClock vc = new VectorClock();
        vc.clock.putAll(this.clock);
        return vc;
    }

    /** Increments this node’s logical time. */
    public void increment(String nodeId) {
        clock.put(nodeId, clock.getOrDefault(nodeId, 0) + 1);
    }

    /**
     * Merges multiple parent clocks using element-wise max.
     */
    public static VectorClock mergeAll(List<VectorClock> parents) {
        VectorClock merged = new VectorClock();
        for (VectorClock p : parents) {
            for (var e : p.clock.entrySet()) {
                merged.clock.put(
                        e.getKey(),
                        Math.max(merged.clock.getOrDefault(e.getKey(), 0), e.getValue())
                );
            }
        }
        return merged;
    }

    /**
     * Returns true if this clock is causally before (≤) another.
     */
    public boolean isAncestorOf(VectorClock other) {
        Set<String> all = new HashSet<>(clock.keySet());
        all.addAll(other.clock.keySet());

        for (String n : all) {
            if (clock.getOrDefault(n, 0) > other.clock.getOrDefault(n, 0))
                return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return clock.toString();
    }
}

/**
 * Value paired with a vector clock.
 */
record VersionedValue(String value, VectorClock vClock) {}

/**
 * Result returned to client on GET.
 */
record GetResult(List<VersionedValue> siblings) {}