package Raft;

import java.util.*;

/**
 * RaftCluster
 *
 * This class represents the cluster membership.
 * It is NOT a Raft concept per se — it just:
 *
 *  - stores all RaftNode instances
 *  - allows nodes to find and call each other
 *
 * Think of this as a stand-in for:
 *   - DNS
 *   - service discovery
 *   - networking layer
 */
public class RaftCluster {

    private final List<RaftNode> nodes = new ArrayList<>();

    /**
     * Add a node to the cluster.
     * Called during setup before threads start.
     */
    public void addNode(RaftNode node) {
        nodes.add(node);
    }

    /**
     * Return all nodes in the cluster.
     */
    public List<RaftNode> getNodes() {
        return nodes;
    }

    /**
     * Lookup node by ID.
     * We assume nodeId == index for simplicity.
     */
    public RaftNode getNode(int id) {
        return nodes.get(id);
    }

    /**
     * Cluster size (used to compute majority).
     */
    public int size() {
        return nodes.size();
    }
}