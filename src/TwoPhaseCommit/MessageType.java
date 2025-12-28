package TwoPhaseCommit;

public enum MessageType {
    PREPARE,
    VOTE_YES,
    VOTE_NO,
    COMMIT,
    ABORT,
    ACK
}
