package Raft;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import static Raft.RaftMessages.*;

/**
 * A single Raft server.
 *
 * Runs as its own thread:
 *  - follower: waits for heartbeats
 *  - candidate: starts elections on timeout
 *  - leader: sends heartbeats + replicates log
 */
public class RaftNode extends Thread {

    enum Role { FOLLOWER, CANDIDATE, LEADER }

    private final int id;
    private final RaftCluster cluster;

    /* ================= Persistent State ================= */
    private int currentTerm = 0;
    private Integer votedFor = null;
    private final List<LogEntry> log = new ArrayList<>();

    /* ================= Volatile State ================= */
    private int commitIndex = -1;
    private int lastApplied = -1;

    /* ================= Leader-only State ================= */
    private int[] nextIndex;
    private int[] matchIndex;

    /* ================= Role / Timing ================= */
    private Role role = Role.FOLLOWER;
    private Integer leaderId = null;

    private long lastHeartbeatTime = now();
    private long electionTimeout = randomElectionTimeout();

    private volatile boolean running = true;

    public RaftNode(int id, RaftCluster cluster) {
        this.id = id;
        this.cluster = cluster;
    }

    /* ================= Thread Loop ================= */

    @Override
    public void run() {
        while (running) {
            tick();
            sleepQuietly(20);
        }
    }

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

    /* ================= Election ================= */

    private void startElection() {
        role = Role.CANDIDATE;
        currentTerm++;
        votedFor = id;
        leaderId = null;

        lastHeartbeatTime = now();
        electionTimeout = randomElectionTimeout();

        int votes = 1;
        int majority = cluster.size() / 2 + 1;

        RequestVoteArgs args = new RequestVoteArgs(
                currentTerm, id, lastLogIndex(), lastLogTerm()
        );

        System.out.println("Node " + id + " starts election for term " + currentTerm);

        for (RaftNode peer : cluster.getNodes()) {
            if (peer.id == this.id) continue;

            RequestVoteReply reply = peer.onRequestVote(args);

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

    /* ================= Leader ================= */

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

    private void sendHeartbeats() {
        for (RaftNode peer : cluster.getNodes()) {
            if (peer.id == this.id) continue;
            replicateTo(peer);
        }
    }

    private void replicateTo(RaftNode peer) {
        int ni = nextIndex[peer.id];
        int prevIndex = ni - 1;
        int prevTerm = (prevIndex >= 0) ? log.get(prevIndex).term : 0;

        List<LogEntry> entries = new ArrayList<>();
        for (int i = ni; i < log.size(); i++)
            entries.add(log.get(i));

        AppendEntriesArgs args = new AppendEntriesArgs(
                currentTerm, id, prevIndex, prevTerm,
                entries, commitIndex
        );

        AppendEntriesReply reply = peer.onAppendEntries(args);

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

        int index = args.prevLogIndex + 1;
        for (LogEntry e : args.entries) {
            if (index < log.size()) {
                if (log.get(index).term != e.term) {
                    while (log.size() > index)
                        log.remove(log.size() - 1);
                    log.add(e);
                }
            } else {
                log.add(e);
            }
            index++;
        }

        if (args.leaderCommit > commitIndex)
            commitIndex = Math.min(args.leaderCommit, log.size() - 1);

        return new AppendEntriesReply(currentTerm, true);
    }

    /* ================= Commit & Apply ================= */

    private void advanceCommitIndex() {
        int majority = cluster.size() / 2 + 1;

        for (int i = commitIndex + 1; i < log.size(); i++) {
            if (log.get(i).term != currentTerm) continue;

            int count = 1;
            for (int j = 0; j < cluster.size(); j++) {
                if (j != id && matchIndex[j] >= i)
                    count++;
            }

            if (count >= majority)
                commitIndex = i;
        }
    }

    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            System.out.println("Node " + id + " APPLY " + log.get(lastApplied));
        }
    }

    /* ================= Client ================= */

    public void clientPropose(String command) {
        if (role != Role.LEADER) {
            if (leaderId != null)
                cluster.getNode(leaderId).clientPropose(command);
            return;
        }

        log.add(new LogEntry(currentTerm, command));
        sendHeartbeats();
    }

    /* ================= Helpers ================= */

    private void becomeFollower(int term, Integer leader) {
        role = Role.FOLLOWER;
        currentTerm = term;
        votedFor = null;
        leaderId = leader;
        lastHeartbeatTime = now();
        electionTimeout = randomElectionTimeout();
    }

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