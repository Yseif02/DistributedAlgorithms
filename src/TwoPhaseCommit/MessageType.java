package TwoPhaseCommit;

/**
 * MessageType
 *
 * Enumerates all messages used in the Two-Phase Commit protocol.
 *
 * These messages directly correspond to the standard 2PC steps:
 *
 * PHASE 1 (VOTING):
 *  - PREPARE: coordinator asks participants if they can commit
 *  - VOTE_YES / VOTE_NO: participant response
 *
 * PHASE 2 (DECISION):
 *  - COMMIT: coordinator instructs participants to commit
 *  - ABORT: coordinator instructs participants to abort
 *
 * CLEANUP:
 *  - ACK: participant acknowledges completion
 */
enum MessageType {
    PREPARE,
    VOTE_YES,
    VOTE_NO,
    COMMIT,
    ABORT,
    ACK
}
