package Raft;

import java.util.*;

public class RaftCluster {

    private final List<RaftNode> nodes = new ArrayList<>();

    public void addNode(RaftNode node) {
        nodes.add(node);
    }

    public List<RaftNode> getNodes() {
        return nodes;
    }

    public RaftNode getNode(int id) {
        return nodes.get(id);
    }

    public int size() {
        return nodes.size();
    }
}
