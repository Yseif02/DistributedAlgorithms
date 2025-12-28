package Raft;

import java.util.*;

/**
 * RaftMessages
 *
 * This class contains ONLY the message / data types used
 * for Raft RPCs. These objects are "magically" sent between
 * nodes (we skip serialization/networking).
 *
 * Keeping these separated helps clarify:
 *   - what data is exchanged
 *   - what logic lives in RaftNode
 */
public class RaftMessages {

    /**
     * A single log entry in the replicated log.
     *
     * Each entry contains:
     *  - term: the leader term when the entry was created
     *  - command: opaque command for the state machine
     *
     * Raft guarantees:
     *  - logs are identical (same entries, same order) on all servers
     *    once entries are committed
     */
    public static class LogEntry {
        public final int term;
        public final String command;

        public LogEntry(int term, String command) {
            this.term = term;
            this.command = command;
        }

        @Override
        public String toString() {
            return "(" + term + ":" + command + ")";
        }
    }

    /* =========================================================
     * RequestVote RPC
     *
     * Used during leader election.
     * Candidates send RequestVote to all peers.
     * ========================================================= */

    /**
     * Arguments sent by a candidate when requesting votes.
     *
     * Fields come directly from the Raft paper:
     *  - term: candidate’s current term
     *  - candidateId: who is requesting the vote
     *  - lastLogIndex / lastLogTerm: used to enforce
     *    "up-to-date log" rule (safety property)
     */
    public static class RequestVoteArgs {
        public final int term;
        public final int candidateId;
        public final int lastLogIndex;
        public final int lastLogTerm;

        public RequestVoteArgs(int term, int candidateId,
                               int lastLogIndex, int lastLogTerm) {
            this.term = term;
            this.candidateId = candidateId;
            this.lastLogIndex = lastLogIndex;
            this.lastLogTerm = lastLogTerm;
        }
    }

    /**
     * Reply to RequestVote.
     *
     *  - term: receiver’s current term
     *  - voteGranted: whether vote was granted
     *
     * If reply.term > candidate.term,
     * the candidate must step down.
     */
    public static class RequestVoteReply {
        public final int term;
        public final boolean voteGranted;

        public RequestVoteReply(int term, boolean voteGranted) {
            this.term = term;
            this.voteGranted = voteGranted;
        }
    }

    /* =========================================================
     * AppendEntries RPC
     *
     * Used for:
     *   - heartbeats
     *   - log replication
     * ========================================================= */

    /**
     * Arguments sent by leader to followers.
     *
     * This single RPC serves two purposes:
     *  1. Heartbeat (entries list empty)
     *  2. Log replication (entries list non-empty)
     */
    public static class AppendEntriesArgs {
        public final int term;
        public final int leaderId;

        // Used to check log consistency
        public final int prevLogIndex;
        public final int prevLogTerm;

        // New entries to append (may be empty)
        public final List<LogEntry> entries;

        // Leader’s commit index
        public final int leaderCommit;

        public AppendEntriesArgs(int term, int leaderId,
                                 int prevLogIndex, int prevLogTerm,
                                 List<LogEntry> entries,
                                 int leaderCommit) {
            this.term = term;
            this.leaderId = leaderId;
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
            this.entries = entries;
            this.leaderCommit = leaderCommit;
        }
    }

    /**
     * Reply to AppendEntries.
     *
     *  - success: false if log inconsistency detected
     *  - term: receiver’s current term
     *
     * If success == false, leader backs up nextIndex and retries.
     */
    public static class AppendEntriesReply {
        public final int term;
        public final boolean success;

        public AppendEntriesReply(int term, boolean success) {
            this.term = term;
            this.success = success;
        }
    }
}