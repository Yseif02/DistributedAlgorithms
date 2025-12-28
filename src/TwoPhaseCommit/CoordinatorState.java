package TwoPhaseCommit;

/**
 * CoordinatorState
 *
 * Represents the coordinator’s progress
 * through the Two-Phase Commit protocol.
 */
enum CoordinatorState {
    INIT,
    WAITING_FOR_VOTES,
    COMMITTING,
    ABORTING
}