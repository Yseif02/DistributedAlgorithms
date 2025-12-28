package TwoPhaseCommit;

public enum CoordinatorState {
    INIT,
    WAITING_FOR_VOTES,
    COMMITTING,
    ABORTING
}
