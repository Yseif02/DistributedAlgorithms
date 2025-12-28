package TwoPhaseCommit;

public class Participant {
    private final int id;
    private ParticipantState state = ParticipantState.INIT;

    private final Coordinator coordinator;

    public Participant(int id, Coordinator coordinator) {
        this.id = id;
        this.coordinator = coordinator;
    }

    /* =======================
       Receive Messages
       ======================= */

    public void receiveMessage(MessageType type) {
        switch (type) {

            case PREPARE:
                onPrepare();
                break;

            case COMMIT:
                onCommit();
                break;

            case ABORT:
                onAbort();
                break;
        }
    }

    /* =======================
       Phase 1: Prepare
       ======================= */

    private void onPrepare() {
        // Pseudocode: check if local transaction can commit
        System.out.println("Participant " + id + " received PREPARE");

        boolean canCommit = checkLocalConstraints();

        if (canCommit) {
            System.out.println("Participant " + id + " votes YES");
            persist("PREPARED");
            state = ParticipantState.PREPARED;
            coordinator.receiveMessage(id, MessageType.VOTE_YES);
        } else {
            System.out.println("Participant " + id + " votes NO");
            persist("ABORTED");
            state = ParticipantState.ABORTED;
            coordinator.receiveMessage(id, MessageType.VOTE_NO);
        }
    }

    /* =======================
       Phase 2: Commit
       ======================= */

    private void onCommit() {
        persist("COMMITTED");
        state = ParticipantState.COMMITTED;

        // Apply transaction changes locally
        applyTransaction();

        coordinator.receiveMessage(id, MessageType.ACK);
    }

    private void onAbort() {
        persist("ABORTED");
        state = ParticipantState.ABORTED;

        rollbackTransaction();

        coordinator.receiveMessage(id, MessageType.ACK);
    }

    /* =======================
       Helpers
       ======================= */

    private boolean checkLocalConstraints() {
        // Example checks:
        // - Locks acquired
        // - No conflicts
        // - Enough resources
        return true; // or false
    }

    private void applyTransaction() {
        // apply changes
    }

    private void rollbackTransaction() {
        // undo changes
    }

    private void persist(String record) {
        // write record to stable storage (log)
    }
}
