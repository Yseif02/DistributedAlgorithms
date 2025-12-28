package Raft;

public class Main {

    public static void main(String[] args) throws Exception {

        RaftCluster cluster = new RaftCluster();

        for (int i = 0; i < 5; i++) {
            RaftNode node = new RaftNode(i, cluster);
            cluster.addNode(node);
        }

        // Start all nodes (threads)
        for (RaftNode node : cluster.getNodes()) {
            node.start();
        }

        // Let leader election happen
        Thread.sleep(2000);

        // Client proposals
        System.out.println("\nCLIENT: set x=1");
        cluster.getNode(2).clientPropose("set x=1");

        Thread.sleep(1000);

        System.out.println("\nCLIENT: set y=2");
        cluster.getNode(4).clientPropose("set y=2");

        Thread.sleep(2000);

        System.exit(0);
    }
}
