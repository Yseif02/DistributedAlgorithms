package TwoPhaseCommit;

import java.util.*;

public class Coordinator {

    private final List<Participant> participants;
    private final Map<Integer, MessageType> votes = new HashMap<>();

    private CoordinatorState state = CoordinatorState.INIT;

    public Coordinator(List<Participant> participants) {
        this.participants = participants;
    }

    /* =======================
       Start Transaction
       ======================= */

    public void startTransaction() {
        state = CoordinatorState.WAITING_FOR_VOTES;
        persist("START_2PC");

        // Phase 1: send PREPARE
        for (Participant p : participants) {
            p.receiveMessage(MessageType.PREPARE);
        }
    }

    /* =======================
       Receive Votes / ACKs
       ======================= */

    public void receiveMessage(int participantId, MessageType type) {
        switch (type) {

            case VOTE_YES:
            case VOTE_NO:
                handleVote(participantId, type);
                break;

            case ACK:
                handleAck(participantId);
                break;
        }
    }

    /* =======================
       Vote Handling
       ======================= */

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

    /* =======================
       Phase 2 Decisions
       ======================= */

    private void commit() {
        state = CoordinatorState.COMMITTING;
        persist("GLOBAL_COMMIT");

        for (Participant p : participants) {
            p.receiveMessage(MessageType.COMMIT);
        }
    }

    private void abort() {
        state = CoordinatorState.ABORTING;
        persist("GLOBAL_ABORT");

        for (Participant p : participants) {
            p.receiveMessage(MessageType.ABORT);
        }
    }

    /* =======================
       ACK Handling
       ======================= */

    private void handleAck(int id) {
        // Track ACKs (omitted for brevity)
        // Once all ACKs received:
        persist("END_2PC");
        state = CoordinatorState.INIT;
    }

    private void persist(String record) {
        // write coordinator log record to stable storage
    }
}
