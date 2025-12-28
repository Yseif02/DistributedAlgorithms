package Dynamo;

public class Main {
    public static void main(String[] args) {

        // Build 5 nodes on a ring (tokens are arbitrary but ordered)
        var nodes = java.util.List.of(
                new DynamoNode("A", new java.math.BigInteger("10")),
                new DynamoNode("B", new java.math.BigInteger("20")),
                new DynamoNode("C", new java.math.BigInteger("30")),
                new DynamoNode("D", new java.math.BigInteger("40")),
                new DynamoNode("E", new java.math.BigInteger("50"))
        );

        // N=3 replicas, R=2 reads, W=2 writes (classic Dynamo-ish demo config)  [oai_citation:16‡07-Replication (1).pdf](sediment://file_00000000679071fd83f7ca12d8d16b10)
        DynamoCluster cluster = new DynamoCluster(new java.util.ArrayList<>(nodes), 3, 2, 2);

        String key = "cart:yaakov";

        // 1) Initial write
        nodes.get(0).clientPut(key, "cart=[eggs]", null);

        // 2) Read (should return 1 version)
        GetResult r1 = nodes.get(3).clientGet(key);
        System.out.println("Read1 siblings = " + r1.siblings());

        // 3) Create divergence: simulate a partition by taking one replica down,
        //    then writing from a different node (another coordinator path).
        nodes.get(1).isUp = false; // node B down (causes missed replica + possible hinted handoff)  [oai_citation:17‡07-Replication (1).pdf](sediment://file_00000000679071fd83f7ca12d8d16b10)

        // Write #2 using context from Read1 (normal update)
        nodes.get(2).clientPut(key, "cart=[eggs,milk]", r1.siblings());

        // Bring B back, but meanwhile do an “independent” write from another node with older context
        nodes.get(1).isUp = true;

        // This write uses *the old* context r1, so it may create a concurrent branch.
        nodes.get(4).clientPut(key, "cart=[eggs,bread]", r1.siblings());

        // 4) Read again: should often return 2 siblings (conflict)
        GetResult r2 = nodes.get(0).clientGet(key);
        System.out.println("Read2 siblings = " + r2.siblings());

        // 5) Client reconciles: app decides merge policy (semantic reconciliation)
        //    Example: union items, then write back using the siblings context (collapses branches)  [oai_citation:18‡07-Replication (1).pdf](sediment://file_00000000679071fd83f7ca12d8d16b10)
        String reconciled = "cart=[eggs,milk,bread]";
        nodes.get(0).clientPut(key, reconciled, r2.siblings());

        GetResult r3 = nodes.get(2).clientGet(key);
        System.out.println("Read3 siblings = " + r3.siblings());

        // 6) Background hinted handoff tick (optional demo)
        for (DynamoNode n : nodes) n.runHintedHandoffTick();
    }
}