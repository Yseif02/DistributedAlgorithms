package TwoPhaseCommit;

/**
 * ParticipantState
 *
 * Represents the local state of a participant
 * during a 2PC transaction.
 *
 * State transitions are monotonic:
 *
 * INIT → PREPARED → COMMITTED
 * INIT → ABORTED
 *
 * Once COMMITTED or ABORTED, the decision is final.
 */
enum ParticipantState {
    INIT,
    PREPARED,
    COMMITTED,
    ABORTED
}
