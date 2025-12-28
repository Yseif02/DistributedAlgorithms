package TwoPhaseCommit;

import java.util.*;

public class Main {

    public static void main(String[] args) {

        int numParticipants = 8;

        // Step 1: Create coordinator (participants added later)
        Coordinator coordinator = new Coordinator(new ArrayList<>());

        // Step 2: Create participants
        List<Participant> participants = new ArrayList<>();
        for (int i = 0; i < numParticipants; i++) {
            participants.add(new Participant(i, coordinator));
        }

        // Step 3: Wire participants into coordinator
        // (This simulates "knowing" cluster membership)
        coordinator = new Coordinator(participants);

        // Step 4: Start the transaction
        System.out.println("=== Starting Two-Phase Commit ===");
        coordinator.startTransaction();

        // Step 5: Print final states
        System.out.println("\n=== Final Participant States ===");
        for (Participant p : participants) {
            System.out.println("Participant " + p + " finished");
        }

        System.out.println("\n=== 2PC Test Complete ===");
    }
}
