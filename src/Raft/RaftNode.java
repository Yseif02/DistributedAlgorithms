package Raft;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import static Raft.RaftMessages.*;

/**
 * RaftNode
 *
 * A single Raft server running in its own thread.
 *
 * Each node independently:
 *  - waits as a follower
 *  - becomes a candidate on election timeout
 *  - becomes leader if it wins a majority
 *  - replicates log entries and commits them
 *
 * This class implements the core Raft protocol logic:
 *   - leader election
 *   - log replication
 *   - commitment and application
 */
public class RaftNode extends Thread {

    /* =========================================================
     * Raft roles
     * ========================================================= */
    enum Role {
        FOLLOWER,   // passive; only responds to RPCs
        CANDIDATE,  // attempting to become leader
        LEADER      // actively replicates log and sends heartbeats
    }

    /* =========================================================
     * Identity and cluster wiring
     * ========================================================= */
    private final int id;                 // unique server ID
    private final RaftCluster cluster;    // cluster membership + lookup

    /* =========================================================
     * Persistent State (would be on stable storage in real Raft)
     * ========================================================= */

    private int currentTerm = 0;
    // The highest term this server has ever seen.
    // Terms are monotonically increasing and define logical time.

    private Integer votedFor = null;
    // The candidate this server voted for in currentTerm.
    // Ensures at most one vote per term per server.

    private final List<LogEntry> log = new ArrayList<>();
    // Replicated log: ordered list of commands.
    // Raft guarantees all committed prefixes are identical on all servers.

    /* =========================================================
     * Volatile State (in-memory only)
     * ========================================================= */

    private int commitIndex = -1;
    // Index of highest log entry known to be committed.

    private int lastApplied = -1;
    // Index of highest log entry applied to the state machine.

    /* =========================================================
     * Leader-only Volatile State
     * ========================================================= */

    private int[] nextIndex;
    // For each follower: the next log index the leader should send.

    private int[] matchIndex;
    // For each follower: highest log index known to be replicated.

    /* =========================================================
     * Role, leader tracking, and timing
     * ========================================================= */

    private Role role = Role.FOLLOWER;

    private Integer leaderId = null;
    // Used by followers to redirect clients.

    private long lastHeartbeatTime = now();
    // Timestamp of last valid heartbeat or RequestVote received.

    private long electionTimeout = randomElectionTimeout();
    // Randomized timeout to reduce split votes.

    private volatile boolean running = true;
    // Used to stop thread cleanly if needed.

    public RaftNode(int id, RaftCluster cluster) {
        this.id = id;
        this.cluster = cluster;
    }

    /* =========================================================
     * run()
     *
     * Entry point for the thread.
     * Each node continuously executes Raft logic until stopped.
     * ========================================================= */
    @Override
    public void run() {
        while (running) {
            tick();
            sleepQuietly(20);
        }
    }

    /* =========================================================
     * tick()
     *
     * Executes one logical iteration of the Raft protocol:
     *
     * 1. Followers / Candidates:
     *    - If election timeout elapsed → start election
     *
     * 2. Leader:
     *    - Send heartbeats / replicate log entries
     *
     * 3. All nodes:
     *    - Apply newly committed log entries
     * ========================================================= */
    private void tick() {
        long elapsed = now() - lastHeartbeatTime;

        if (role != Role.LEADER && elapsed > electionTimeout) {
            startElection();
        }

        if (role == Role.LEADER) {
            sendHeartbeats();
        }

        applyCommittedEntries();
    }

    /* =========================================================
     * startElection()
     *
     * Transitions the node from FOLLOWER → CANDIDATE.
     *
     * Steps:
     *  1. Increment currentTerm
     *  2. Vote for self
     *  3. Reset election timer
     *  4. Send RequestVote RPCs to all peers
     *  5. Become leader if majority votes received
     * ========================================================= */
    private void startElection() {
        role = Role.CANDIDATE;
        currentTerm++;
        votedFor = id;
        leaderId = null;

        lastHeartbeatTime = now();
        electionTimeout = randomElectionTimeout();

        int votes = 1; // vote for self
        int majority = cluster.size() / 2 + 1;

        System.out.println("Node " + id + " starts election for term " + currentTerm);

        RequestVoteArgs args = new RequestVoteArgs(
                currentTerm, id, lastLogIndex(), lastLogTerm()
        );

        for (RaftNode peer : cluster.getNodes()) {
            if (peer.id == this.id) continue;

            RequestVoteReply reply = peer.onRequestVote(args);

            // If we see a higher term, immediately step down
            if (reply.term > currentTerm) {
                becomeFollower(reply.term, null);
                return;
            }

            if (reply.voteGranted) votes++;

            if (votes >= majority) {
                becomeLeader();
                return;
            }
        }
    }

    /* =========================================================
     * onRequestVote(...)
     *
     * Handles incoming RequestVote RPC.
     *
     * Voting rules:
     *  1. Reject if term < currentTerm
     *  2. Update term and step down if term > currentTerm
     *  3. Grant vote if:
     *      a) Haven’t voted yet (or voted for this candidate)
     *      b) Candidate log is at least as up-to-date
     * ========================================================= */
    private RequestVoteReply onRequestVote(RequestVoteArgs args) {
        if (args.term < currentTerm)
            return new RequestVoteReply(currentTerm, false);

        if (args.term > currentTerm)
            becomeFollower(args.term, null);

        boolean upToDate =
                args.lastLogTerm > lastLogTerm() ||
                        (args.lastLogTerm == lastLogTerm()
                                && args.lastLogIndex >= lastLogIndex());

        boolean canVote = (votedFor == null || votedFor == args.candidateId);

        if (canVote && upToDate) {
            votedFor = args.candidateId;
            lastHeartbeatTime = now();
            electionTimeout = randomElectionTimeout();
            return new RequestVoteReply(currentTerm, true);
        }

        return new RequestVoteReply(currentTerm, false);
    }

    /* =========================================================
     * becomeLeader()
     *
     * Called once a candidate wins a majority of votes.
     *
     * Initializes leader-only state and immediately sends
     * heartbeats to establish authority.
     * ========================================================= */
    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = id;

        nextIndex = new int[cluster.size()];
        matchIndex = new int[cluster.size()];

        for (int i = 0; i < cluster.size(); i++) {
            nextIndex[i] = log.size();
            matchIndex[i] = -1;
        }

        System.out.println("Node " + id + " becomes LEADER for term " + currentTerm);
        sendHeartbeats();
    }

    /* =========================================================
     * sendHeartbeats()
     *
     * Leader periodically sends AppendEntries RPCs
     * (with empty entry lists) to all followers.
     *
     * This:
     *  - maintains leadership
     *  - conveys commitIndex
     *  - drives log replication
     * ========================================================= */
    private void sendHeartbeats() {
        for (RaftNode peer : cluster.getNodes()) {
            if (peer.id == this.id) continue;
            replicateTo(peer);
        }
    }

    /* =========================================================
     * replicateTo(...)
     *
     * Sends AppendEntries RPC to a specific follower.
     *
     * If follower rejects due to log inconsistency,
     * leader backs up nextIndex and retries later.
     * ========================================================= */
    private void replicateTo(RaftNode peer) {
        int ni = nextIndex[peer.id];
        int prevIndex = ni - 1;
        int prevTerm = (prevIndex >= 0) ? log.get(prevIndex).term : 0;

        List<LogEntry> entries = new ArrayList<>();
        for (int i = ni; i < log.size(); i++)
            entries.add(log.get(i));

        AppendEntriesReply reply =
                peer.onAppendEntries(new AppendEntriesArgs(
                        currentTerm, id, prevIndex, prevTerm,
                        entries, commitIndex
                ));

        if (reply.term > currentTerm) {
            becomeFollower(reply.term, null);
            return;
        }

        if (reply.success) {
            nextIndex[peer.id] = log.size();
            matchIndex[peer.id] = log.size() - 1;
            advanceCommitIndex();
        } else {
            nextIndex[peer.id] = Math.max(0, nextIndex[peer.id] - 1);
        }
    }

    /* =========================================================
     * onAppendEntries(...)
     *
     * Handles AppendEntries RPC on followers.
     *
     * Enforces:
     *  - term monotonicity
     *  - log consistency
     *  - leader authority
     *
     * Also updates commitIndex based on leaderCommit.
     * ========================================================= */
    private AppendEntriesReply onAppendEntries(AppendEntriesArgs args) {
        if (args.term < currentTerm)
            return new AppendEntriesReply(currentTerm, false);

        becomeFollower(args.term, args.leaderId);

        if (args.prevLogIndex >= 0) {
            if (args.prevLogIndex >= log.size())
                return new AppendEntriesReply(currentTerm, false);

            if (log.get(args.prevLogIndex).term != args.prevLogTerm)
                return new AppendEntriesReply(currentTerm, false);
        }

        int idx = args.prevLogIndex + 1;
        for (LogEntry e : args.entries) {
            if (idx < log.size()) {
                if (log.get(idx).term != e.term) {
                    while (log.size() > idx)
                        log.remove(log.size() - 1);
                    log.add(e);
                }
            } else {
                log.add(e);
            }
            idx++;
        }

        if (args.leaderCommit > commitIndex)
            commitIndex = Math.min(args.leaderCommit, log.size() - 1);

        return new AppendEntriesReply(currentTerm, true);
    }

    /* =========================================================
     * advanceCommitIndex()
     *
     * Leader-only rule:
     *
     * If a log entry from the current term has been replicated
     * on a majority of servers, it can be committed.
     * ========================================================= */
    private void advanceCommitIndex() {
        int majority = cluster.size() / 2 + 1;

        for (int i = commitIndex + 1; i < log.size(); i++) {
            if (log.get(i).term != currentTerm) continue;

            int count = 1; // leader itself
            for (int j = 0; j < cluster.size(); j++) {
                if (j != id && matchIndex[j] >= i)
                    count++;
            }

            if (count >= majority)
                commitIndex = i;
        }
    }

    /* =========================================================
     * applyCommittedEntries()
     *
     * Applies committed log entries to the state machine
     * in strict order.
     *
     * This guarantees linearizability.
     * ========================================================= */
    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            System.out.println("Node " + id + " APPLY " + log.get(lastApplied));
        }
    }

    /* =========================================================
     * clientPropose(...)
     *
     * Entry point for client commands.
     *
     * Clients may contact any node:
     *  - Followers redirect to leader
     *  - Leader appends command to log and replicates
     * ========================================================= */
    public void clientPropose(String command) {
        if (role != Role.LEADER) {
            if (leaderId != null)
                cluster.getNode(leaderId).clientPropose(command);
            return;
        }

        log.add(new LogEntry(currentTerm, command));
        sendHeartbeats();
    }

    /* =========================================================
     * becomeFollower(...)
     *
     * Converts server to follower role.
     * Used when discovering a higher term or valid leader.
     * ========================================================= */
    private void becomeFollower(int term, Integer leader) {
        role = Role.FOLLOWER;
        currentTerm = term;
        votedFor = null;
        leaderId = leader;
        lastHeartbeatTime = now();
        electionTimeout = randomElectionTimeout();
    }

    /* =========================================================
     * Utility helpers
     * ========================================================= */

    private int lastLogIndex() { return log.size() - 1; }
    private int lastLogTerm() { return log.isEmpty() ? 0 : log.get(log.size() - 1).term; }

    private static long now() { return System.currentTimeMillis(); }

    private static long randomElectionTimeout() {
        return ThreadLocalRandom.current().nextLong(300, 600);
    }

    private static void sleepQuietly(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }
}