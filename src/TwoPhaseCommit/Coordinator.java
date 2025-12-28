package TwoPhaseCommit;

import java.util.*;

/**
 * Coordinator
 *
 * Drives the Two-Phase Commit protocol.
 *
 * Responsibilities:
 *  - start transactions
 *  - collect votes
 *  - make the global decision
 *  - notify participants
 *
 * The coordinator is a SINGLE POINT OF FAILURE
 * in classic 2PC.
 */
class Coordinator {

    private final List<Participant> participants;
    private final Map<Integer, MessageType> votes = new HashMap<>();

    private CoordinatorState state = CoordinatorState.INIT;

    public Coordinator(List<Participant> participants) {
        this.participants = participants;
    }

    /* =========================================================
     * startTransaction()
     *
     * Begins a new distributed transaction.
     *
     * Steps:
     *  1. Write START record to log
     *  2. Send PREPARE to all participants
     * ========================================================= */
    public void startTransaction() {
        state = CoordinatorState.WAITING_FOR_VOTES;
        persist("START_2PC");

        for (Participant p : participants) {
            p.receiveMessage(MessageType.PREPARE);
        }
    }

    /* =========================================================
     * receiveMessage(...)
     *
     * Entry point for all messages sent by participants.
     * ========================================================= */
    public void receiveMessage(int participantId, MessageType type) {
        switch (type) {
            case VOTE_YES, VOTE_NO -> handleVote(participantId, type);
            case ACK -> handleAck(participantId);
        }
    }

    /* =========================================================
     * handleVote(...)
     *
     * Collects votes during PHASE 1.
     *
     * Rules:
     *  - If ANY VOTE_NO → GLOBAL_ABORT
     *  - If ALL VOTE_YES → GLOBAL_COMMIT
     * ========================================================= */
    private void handleVote(int id, MessageType vote) {
        votes.put(id, vote);

        if (vote == MessageType.VOTE_NO) {
            abort();
            return;
        }

        if (votes.size() == participants.size()) {
            commit();
        }
    }

    /* =========================================================
     * commit()
     *
     * PHASE 2 — COMMIT
     *
     * Steps:
     *  1. Write GLOBAL_COMMIT to log
     *  2. Send COMMIT to all participants
     * ========================================================= */
    private void commit() {
        state = CoordinatorState.COMMITTING;
        persist("GLOBAL_COMMIT");

        for (Participant p : participants) {
            p.receiveMessage(MessageType.COMMIT);
        }
    }

    /* =========================================================
     * abort()
     *
     * PHASE 2 — ABORT
     *
     * Steps:
     *  1. Write GLOBAL_ABORT to log
     *  2. Send ABORT to all participants
     * ========================================================= */
    private void abort() {
        state = CoordinatorState.ABORTING;
        persist("GLOBAL_ABORT");

        for (Participant p : participants) {
            p.receiveMessage(MessageType.ABORT);
        }
    }

    /* =========================================================
     * handleAck(...)
     *
     * Receives completion acknowledgements
     * from participants.
     *
     * Once all ACKs are received, transaction ends.
     * ========================================================= */
    private void handleAck(int id) {
        // Track ACKs (omitted for simplicity)
        persist("END_2PC");
        state = CoordinatorState.INIT;
    }

    /** Writes coordinator state to stable storage. */
    private void persist(String record) {
        // write to disk
    }
}