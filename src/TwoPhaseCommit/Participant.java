package TwoPhaseCommit;

/**
 * Participant
 *
 * Represents a participant in the Two-Phase Commit protocol.
 *
 * A participant:
 *  - owns some local resource or data
 *  - decides whether it can commit a transaction
 *  - must obey the coordinator’s final decision
 *
 * Participants are NOT allowed to unilaterally commit.
 */
class Participant {

    private final int id;
    private ParticipantState state = ParticipantState.INIT;

    private final Coordinator coordinator;

    public Participant(int id, Coordinator coordinator) {
        this.id = id;
        this.coordinator = coordinator;
    }

    /* =========================================================
     * receiveMessage(...)
     *
     * Entry point for all messages sent by the coordinator.
     *
     * Dispatches to the appropriate handler based on message type.
     * ========================================================= */
    public void receiveMessage(MessageType type) {
        switch (type) {
            case PREPARE -> onPrepare();
            case COMMIT -> onCommit();
            case ABORT -> onAbort();
        }
    }

    /* =========================================================
     * onPrepare()
     *
     * PHASE 1 — VOTING
     *
     * The coordinator is asking:
     *   “Can you commit this transaction?”
     *
     * Participant must:
     *  1. Check local constraints
     *  2. If YES:
     *      - write PREPARED to stable storage
     *      - reply VOTE_YES
     *  3. If NO:
     *      - write ABORTED
     *      - reply VOTE_NO
     *
     * Once PREPARED, the participant is BLOCKED
     * until it hears COMMIT or ABORT.
     * ========================================================= */
    private void onPrepare() {
        boolean canCommit = checkLocalConstraints();

        if (canCommit) {
            persist("PREPARED");
            state = ParticipantState.PREPARED;
            coordinator.receiveMessage(id, MessageType.VOTE_YES);
        } else {
            persist("ABORTED");
            state = ParticipantState.ABORTED;
            coordinator.receiveMessage(id, MessageType.VOTE_NO);
        }
    }

    /* =========================================================
     * onCommit()
     *
     * PHASE 2 — COMMIT
     *
     * Coordinator has decided GLOBAL_COMMIT.
     *
     * Participant must:
     *  1. Write COMMITTED to stable storage
     *  2. Apply transaction changes locally
     *  3. Send ACK to coordinator
     *
     * This decision is FINAL.
     * ========================================================= */
    private void onCommit() {
        persist("COMMITTED");
        state = ParticipantState.COMMITTED;

        applyTransaction();
        coordinator.receiveMessage(id, MessageType.ACK);
    }

    /* =========================================================
     * onAbort()
     *
     * PHASE 2 — ABORT
     *
     * Coordinator has decided GLOBAL_ABORT.
     *
     * Participant must:
     *  1. Write ABORTED to stable storage
     *  2. Roll back any tentative work
     *  3. Send ACK to coordinator
     *
     * This decision is FINAL.
     * ========================================================= */
    private void onAbort() {
        persist("ABORTED");
        state = ParticipantState.ABORTED;

        rollbackTransaction();
        coordinator.receiveMessage(id, MessageType.ACK);
    }

    /* =========================================================
     * Helper methods (placeholders)
     * ========================================================= */

    /**
     * Checks whether the participant can commit.
     *
     * Examples:
     *  - locks available
     *  - no conflicts
     *  - sufficient resources
     */
    private boolean checkLocalConstraints() {
        return true; // or false
    }

    /** Applies the transaction to local state. */
    private void applyTransaction() {
        // apply changes
    }

    /** Rolls back any tentative local changes. */
    private void rollbackTransaction() {
        // undo changes
    }

    /**
     * Writes a record to stable storage.
     *
     * In real systems this is a WAL (write-ahead log).
     */
    private void persist(String record) {
        // write to disk
    }
}