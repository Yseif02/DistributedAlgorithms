package Raft;

import java.util.*;

public class RaftMessages {

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

    /* ================= RequestVote ================= */

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

    public static class RequestVoteReply {
        public final int term;
        public final boolean voteGranted;

        public RequestVoteReply(int term, boolean voteGranted) {
            this.term = term;
            this.voteGranted = voteGranted;
        }
    }

    /* ================= AppendEntries ================= */

    public static class AppendEntriesArgs {
        public final int term;
        public final int leaderId;
        public final int prevLogIndex;
        public final int prevLogTerm;
        public final List<LogEntry> entries;
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

    public static class AppendEntriesReply {
        public final int term;
        public final boolean success;

        public AppendEntriesReply(int term, boolean success) {
            this.term = term;
            this.success = success;
        }
    }
}
