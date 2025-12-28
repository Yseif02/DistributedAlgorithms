package RaftV1;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * SIMPLE RAFT implementation.
 *
 * Goal: replicate a log across a cluster using a strong leader.
 * - Leader election: follower -> candidate -> leader on election timeout
 * - Log replication: leader sends AppendEntries (also heartbeat)
 * - Commitment: entry committed once replicated on a majority, then applied to state machine
 *
 * This follows the key Raft pseudo-code structure:
 *   Persistent state on all servers:
 *     - currentTerm, votedFor, log[]
 *   Volatile state on all servers:
 *     - commitIndex, lastApplied
 *   Volatile state on leaders:
 *     - nextIndex[follower], matchIndex[follower]
 *
 * (See “Pseudo Code” state slide.)
 */
public class SimpleRaftV1 {

    /* =========================
     * Types and RPC definitions
     * ========================= */

    enum Role { FOLLOWER, CANDIDATE, LEADER }

    /** A single replicated log entry: command + term */
    static class LogEntry {
        final int term;
        final String command; // "set x=3" etc. (opaque to Raft)

        LogEntry(int term, String command) {
            this.term = term;
            this.command = command;
        }

        @Override public String toString() { return "(" + term + ":" + command + ")"; }
    }

    /** RequestVote RPC arguments (see RequestVote slide). */
    static class RequestVoteArgs {
        final int term;           // candidate’s term
        final int candidateId;    // candidate requesting vote
        final int lastLogIndex;   // index of candidate’s last log entry
        final int lastLogTerm;    // term of candidate’s last log entry

        RequestVoteArgs(int term, int candidateId, int lastLogIndex, int lastLogTerm) {
            this.term = term;
            this.candidateId = candidateId;
            this.lastLogIndex = lastLogIndex;
            this.lastLogTerm = lastLogTerm;
        }
    }

    /** RequestVote RPC result (see RequestVote slide). */
    static class RequestVoteReply {
        final int term;           // currentTerm, for candidate to update itself
        final boolean voteGranted;

        RequestVoteReply(int term, boolean voteGranted) {
            this.term = term;
            this.voteGranted = voteGranted;
        }
    }

    /** AppendEntries RPC arguments (see AppendEntries slide) */
    static class AppendEntriesArgs {
        final int term;           // leader’s term
        final int leaderId;       // so follower can redirect clients
        final int prevLogIndex;   // index of log entry immediately preceding new ones
        final int prevLogTerm;    // term of prevLogIndex entry
        final List<LogEntry> entries; // empty for heartbeat
        final int leaderCommit;   // leader’s commitIndex

        AppendEntriesArgs(int term, int leaderId, int prevLogIndex, int prevLogTerm,
                          List<LogEntry> entries, int leaderCommit) {
            this.term = term;
            this.leaderId = leaderId;
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
            this.entries = entries;
            this.leaderCommit = leaderCommit;
        }
    }

    /** AppendEntries RPC result (see AppendEntries slide).*/
    static class AppendEntriesReply {
        final int term;       // currentTerm for leader to update itself
        final boolean success;

        AppendEntriesReply(int term, boolean success) {
            this.term = term;
            this.success = success;
        }
    }

    /* =========================
     * The Raft Node
     * ========================= */

    static class RaftNode {
        final int id;
        final List<RaftNode> peers; // cluster membership (magical network)

        // -------------------------
        // Persistent state (stable)
        // -------------------------
        int currentTerm = 0;            // monotonic
        Integer votedFor = null;        // candidateId voted for in currentTerm (or null)
        final List<LogEntry> log = new ArrayList<>(); // 1-indexed in Raft papers; we’ll treat as 0-indexed array list

        // -------------------------
        // Volatile state (in memory)
        // -------------------------
        int commitIndex = -1;   // highest log index known committed (init -1)
        int lastApplied = -1;   // highest log index applied to state machine (init -1)

        // -------------------------
        // Leader-only volatile state
        // -------------------------
        int[] nextIndex;        // for each follower, next log index to send
        int[] matchIndex;       // for each follower, highest log index known replicated

        // -------------------------
        // Role + leader tracking
        // -------------------------
        Role role = Role.FOLLOWER;
        Integer leaderId = null;

        // -------------------------
        // Timing (simplified)
        // -------------------------
        long lastHeardFromLeaderOrCandidateMs = now();
        long electionTimeoutMs = randomElectionTimeout();

        RaftNode(int id, List<RaftNode> peers) {
            this.id = id;
            this.peers = peers;
        }

        /* =========================================================
         * Helpers: time, random timeout, log term/index access
         * ========================================================= */

        static long now() { return System.currentTimeMillis(); }

        static long randomElectionTimeout() {
            // Slides: randomized election timeouts to avoid split vote.
            // Use a range like [300..600]ms for toy simulation.
            return ThreadLocalRandom.current().nextLong(300, 600);
        }

        int lastLogIndex() { return log.size() - 1; }

        int lastLogTerm() {
            if (log.isEmpty()) return 0;
            return log.get(log.size() - 1).term;
        }

        int termAt(int index) {
            if (index < 0 || index >= log.size()) return 0;
            return log.get(index).term;
        }

        /* =========================================================
         * Tick loop (drives elections + heartbeats)
         * In a real system this would be a thread/timer.
         * ========================================================= */

        public void tick() {
            long elapsed = now() - lastHeardFromLeaderOrCandidateMs;

            if (role != Role.LEADER && elapsed > electionTimeoutMs) {
                // Follower election timeout => become candidate and start election.
                startElection();
            }

            if (role == Role.LEADER) {
                // Leader sends periodic heartbeats to maintain authority.
                sendHeartbeats();
            }

            // All servers: if commitIndex > lastApplied, apply entries in order.
            applyCommittedEntries();
        }

        /* =========================================================
         * Role transitions
         * ========================================================= */

        void becomeFollower(int newTerm, Integer newLeaderId) {
            role = Role.FOLLOWER;
            leaderId = newLeaderId;

            // If RPC contains higher term, update currentTerm and convert to follower.
            if (newTerm > currentTerm) {
                currentTerm = newTerm;
                votedFor = null;
            }

            // Reset election timer because we heard from a valid leader/candidate.
            lastHeardFromLeaderOrCandidateMs = now();
            electionTimeoutMs = randomElectionTimeout();
        }

        void becomeLeader() {
            role = Role.LEADER;
            leaderId = id;

            // Initialize leader volatile state: nextIndex = lastLogIndex+1; matchIndex = -1.
            nextIndex = new int[peers.size()];
            matchIndex = new int[peers.size()];
            for (int i = 0; i < peers.size(); i++) {
                nextIndex[i] = log.size(); // "index of next entry to send"
                matchIndex[i] = -1;
            }

            // On election: send initial empty AppendEntries (heartbeat).
            sendHeartbeats();
        }

        /* =========================================================
         * Leader election (candidate behavior)
         * ========================================================= */

        void startElection() {
            role = Role.CANDIDATE;

            // Candidate rules: increment term, vote for self, reset election timer, send RequestVote to all.
            currentTerm++;
            votedFor = id;
            lastHeardFromLeaderOrCandidateMs = now();
            electionTimeoutMs = randomElectionTimeout();

            int votes = 1; // vote for self
            int majority = (peers.size() / 2) + 1;

            RequestVoteArgs args = new RequestVoteArgs(
                    currentTerm,
                    id,
                    lastLogIndex(),
                    lastLogTerm()
            );

            // Send RequestVote RPCs to all other servers.
            for (RaftNode p : peers) {
                if (p.id == this.id) continue;
                RequestVoteReply reply = p.receiveRequestVote(args);

                // If reply.term > currentTerm => step down.
                if (reply.term > currentTerm) {
                    becomeFollower(reply.term, null);
                    return;
                }

                if (role != Role.CANDIDATE) return; // might have stepped down in the meantime

                if (reply.voteGranted) votes++;

                if (votes >= majority) {
                    // Candidate wins if receives majority votes.
                    becomeLeader();
                    return;
                }
            }

            // If no winner (split vote), timeout will trigger a new election later.
        }

        /**
         * RequestVote receiver implementation:
         * 1) reply false if term < currentTerm
         * 2) if votedFor is null or candidateId, and candidate’s log is at least as up-to-date: grant vote
         * (Matches RequestVote slide.)
         */
        RequestVoteReply receiveRequestVote(RequestVoteArgs args) {
            // If candidate term is older, reject immediately.
            if (args.term < currentTerm) {
                return new RequestVoteReply(currentTerm, false);
            }

            // If term is newer, update our term and convert to follower.
            if (args.term > currentTerm) {
                becomeFollower(args.term, null);
            }

            // At this point, args.term == currentTerm.
            // Grant vote iff:
            //  - we haven't voted yet this term OR we already voted for this candidate
            //  - candidate’s log is at least as up-to-date as ours
            boolean canVote = (votedFor == null || votedFor == args.candidateId);
            boolean upToDate = isCandidateLogUpToDate(args.lastLogIndex, args.lastLogTerm);

            if (canVote && upToDate) {
                votedFor = args.candidateId;
                lastHeardFromLeaderOrCandidateMs = now(); // we heard from a candidate
                electionTimeoutMs = randomElectionTimeout();
                return new RequestVoteReply(currentTerm, true);
            } else {
                return new RequestVoteReply(currentTerm, false);
            }
        }

        /**
         * “Up-to-date” check (Raft safety):
         * Candidate log is at least as up-to-date if:
         * - candidate.lastLogTerm > my.lastLogTerm, OR
         * - terms equal AND candidate.lastLogIndex >= my.lastLogIndex
         *
         * (Slide: vote granted iff candidate log is up to date.)
         */
        boolean isCandidateLogUpToDate(int candLastIndex, int candLastTerm) {
            int myLastTerm = lastLogTerm();
            if (candLastTerm != myLastTerm) return candLastTerm > myLastTerm;
            return candLastIndex >= lastLogIndex();
        }

        /* =========================================================
         * AppendEntries: heartbeats + log replication
         * ========================================================= */

        void sendHeartbeats() {
            // Heartbeat = AppendEntries with empty entries list.
            for (RaftNode p : peers) {
                if (p.id == this.id) continue;
                replicateToFollower(p.id); // send entries starting at nextIndex[follower]
            }
        }

        /**
         * Leader replication logic:
         * If last log index >= nextIndex[follower], send AppendEntries starting at nextIndex.
         * If AppendEntries fails due to inconsistency: decrement nextIndex and retry.
         */
        void replicateToFollower(int followerId) {
            if (role != Role.LEADER) return;

            RaftNode follower = peers.get(followerId);
            int ni = nextIndex[followerId]; // next entry index to send

            int prevIndex = ni - 1;
            int prevTerm = termAt(prevIndex);

            List<LogEntry> entriesToSend = new ArrayList<>();
            // Send all entries from ni to end (could be batched; here we send the suffix).
            for (int i = ni; i < log.size(); i++) entriesToSend.add(log.get(i));

            AppendEntriesArgs args = new AppendEntriesArgs(
                    currentTerm,
                    id,
                    prevIndex,
                    prevTerm,
                    entriesToSend,   // empty => heartbeat
                    commitIndex
            );

            AppendEntriesReply reply = follower.receiveAppendEntries(args);

            // If follower has higher term, step down.
            if (reply.term > currentTerm) {
                becomeFollower(reply.term, null);
                return;
            }

            if (role != Role.LEADER) return;

            if (reply.success) {
                // Success means follower now matches leader up through the new entries.
                // Update nextIndex and matchIndex.
                int newMatch = args.prevLogIndex + args.entries.size();
                matchIndex[followerId] = Math.max(matchIndex[followerId], newMatch);
                nextIndex[followerId] = matchIndex[followerId] + 1;

                // After successful replication, leader can advance commitIndex if a majority replicated an entry
                advanceCommitIndexIfPossible();
            } else {
                // Failure due to log inconsistency:
                // Decrement nextIndex and retry until logs match.
                nextIndex[followerId] = Math.max(0, nextIndex[followerId] - 1);
                // In a real implementation you would retry asynchronously; here we can retry immediately:
                // replicateToFollower(followerId);
            }
        }

        /**
         * AppendEntries receiver implementation (follower side):
         * 1) reply false if term < currentTerm
         * 2) reply false if log doesn’t contain entry at prevLogIndex with term = prevLogTerm
         * 3) if conflict: delete existing entry and all that follow
         * 4) append any new entries not already in log
         * 5) if leaderCommit > commitIndex: commitIndex = min(leaderCommit, index of last new entry)
         *
         * (Matches AppendEntries slides.)
         */
        AppendEntriesReply receiveAppendEntries(AppendEntriesArgs args) {
            // Step 1: term check
            if (args.term < currentTerm) {
                return new AppendEntriesReply(currentTerm, false);
            }

            // If term is newer (or equal but we’re candidate), recognize leader and step down.
            if (args.term > currentTerm || role != Role.FOLLOWER) {
                becomeFollower(args.term, args.leaderId);
            } else {
                // We are follower in same term: still record leader and reset election timer.
                leaderId = args.leaderId;
                lastHeardFromLeaderOrCandidateMs = now();
                electionTimeoutMs = randomElectionTimeout();
            }

            // Step 2: consistency check for prevLogIndex/prevLogTerm
            if (args.prevLogIndex >= 0) {
                if (args.prevLogIndex >= log.size()) {
                    return new AppendEntriesReply(currentTerm, false);
                }
                if (log.get(args.prevLogIndex).term != args.prevLogTerm) {
                    return new AppendEntriesReply(currentTerm, false);
                }
            }

            // Step 3+4: merge entries: delete conflicts and append new ones
            int insertIndex = args.prevLogIndex + 1;
            for (int i = 0; i < args.entries.size(); i++) {
                int logIndex = insertIndex + i;
                LogEntry incoming = args.entries.get(i);

                if (logIndex < log.size()) {
                    LogEntry existing = log.get(logIndex);
                    if (existing.term != incoming.term) {
                        // Conflict: delete existing entry and everything after it.
                        while (log.size() > logIndex) log.remove(log.size() - 1);
                        log.add(incoming);
                    } else {
                        // Same term at this index => already have it; do nothing
                    }
                } else {
                    // We’re beyond current end => append
                    log.add(incoming);
                }
            }

            // Step 5: update commitIndex based on leaderCommit
            if (args.leaderCommit > commitIndex) {
                int lastNewEntryIndex = log.size() - 1;
                commitIndex = Math.min(args.leaderCommit, lastNewEntryIndex);
            }

            return new AppendEntriesReply(currentTerm, true);
        }

        /* =========================================================
         * Commitment rule (leader) + applying to state machine
         * ========================================================= */

        /**
         * Leader rule:
         * If there exists an N > commitIndex such that:
         *  - a majority have matchIndex[i] >= N
         *  - log[N].term == currentTerm
         * then set commitIndex = N.
         *
         * (This is the key leader commit rule on the “Rules for Servers: Leaders” slide.)
         */
        void advanceCommitIndexIfPossible() {
            if (role != Role.LEADER) return;

            int majority = (peers.size() / 2) + 1;

            // Consider candidate commit positions from current commitIndex+1 up to lastLogIndex.
            for (int N = commitIndex + 1; N <= lastLogIndex(); N++) {
                // Only commit entries from currentTerm via this rule (Raft safety).
                if (termAt(N) != currentTerm) continue;

                int count = 1; // leader itself implicitly has the entry
                for (int i = 0; i < peers.size(); i++) {
                    if (i == id) continue;
                    if (matchIndex != null && matchIndex[i] >= N) count++;
                }

                if (count >= majority) {
                    commitIndex = N;
                }
            }
        }

        /**
         * All servers:
         * If commitIndex > lastApplied:
         *   lastApplied++
         *   apply log[lastApplied] to the state machine
         *
         * (Rule for all servers slide.)
         */
        void applyCommittedEntries() {
            while (lastApplied < commitIndex) {
                lastApplied++;
                LogEntry e = log.get(lastApplied);

                // “Apply to state machine” = execute command deterministically.
                // In a KV-store, this might be: apply("set x=3") to a map.
                applyToStateMachine(e.command);
            }
        }

        void applyToStateMachine(String command) {
            // Placeholder for real service logic:
            // e.g. parse command, mutate replicated state
            // For demo, just print:
            System.out.println("Node " + id + " APPLY idx=" + lastApplied + " cmd=" + command);
        }

        /* =========================================================
         * Client interaction (writes must go to leader)
         * ========================================================= */

        /**
         * In Raft, clients send writes to the leader; followers redirect.
         * Slide: leader accepts client commands, appends to log, replicates, commits on majority, then applies.
         */
        public void clientProposeCommand(String command) {
            if (role != Role.LEADER) {
                // redirect to leader if known
                if (leaderId != null) {
                    peers.get(leaderId).clientProposeCommand(command);
                    return;
                }
                throw new RuntimeException("No leader known; client should retry.");
            }

            // Leader appends command to local log
            log.add(new LogEntry(currentTerm, command));

            // Replicate to followers (in real system: async + retry indefinitely).
            for (RaftNode p : peers) {
                if (p.id == this.id) continue;
                replicateToFollower(p.id);
            }

            // After replication attempts, leader may have advanced commitIndex.
            // Followers learn commitIndex through future AppendEntries (leaderCommit field).
            sendHeartbeats(); // ensures followers get updated leaderCommit quickly
        }

        @Override public String toString() {
            return "RaftNode{id=" + id + ", role=" + role + ", term=" + currentTerm +
                    ", log=" + log + ", commitIndex=" + commitIndex + ", lastApplied=" + lastApplied + "}";
        }
    }

    /* =========================
     * Mini test harness
     * ========================= */

    public static void main(String[] args) {
        // Build a 5-node cluster (odd number to avoid split decisions; majority is N/2+1).
        int n = 5;
        List<RaftNode> nodes = new ArrayList<>();
        for (int i = 0; i < n; i++) nodes.add(new RaftNode(i, nodes));

        // Now that nodes list exists, set each node's peers reference correctly:
        // (We passed 'nodes' list during construction, which is now populated.)

        // Run a simple time simulation:
        // 1) ticks cause election
        // 2) leader starts heartbeating
        // 3) client proposes commands to some node (which redirects to leader)
        for (int step = 0; step < 50; step++) {
            // tick everyone
            for (RaftNode rn : nodes) rn.tick();

            // At step 20 and 30, propose commands via arbitrary node
            if (step == 20) {
                System.out.println("\n=== CLIENT proposes cmd1 via node 3 ===");
                nodes.get(3).clientProposeCommand("set x=1");
            }
            if (step == 30) {
                System.out.println("\n=== CLIENT proposes cmd2 via node 1 ===");
                nodes.get(1).clientProposeCommand("set y=2");
            }

            // Sleep a bit so election timers elapse in wall-clock time
            try { Thread.sleep(50); } catch (InterruptedException ignored) {}
        }

        System.out.println("\n=== FINAL NODE STATES ===");
        for (RaftNode rn : nodes) System.out.println(rn);
    }
}
