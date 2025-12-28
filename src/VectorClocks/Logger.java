package VectorClocks;

import java.util.*;

class Logger {

    private final int numWorkers;

    private final List<Classes.LogEntry> buffer = new ArrayList<>();
    private final List<Classes.LogEntry> globalLog = new ArrayList<>();

    // Tracks the maximum delivered vector clock
    private final int[] deliveredClock;

    public Logger(int numWorkers) {
        this.numWorkers = numWorkers;
        this.deliveredClock = new int[numWorkers];
    }

    /* =======================
       Receiving Messages
       ======================= */

    public void receiveMessage(Classes.LogEntry entry) {
        buffer.add(entry);
        tryDeliver();
    }

    /* =======================
       Causal Delivery Logic
       ======================= */

    private void tryDeliver() {
        boolean progress;

        do {
            progress = false;

            Iterator<Classes.LogEntry> it = buffer.iterator();
            while (it.hasNext()) {
                Classes.LogEntry entry = it.next();

                if (canDeliver(entry)) {
                    deliver(entry);
                    it.remove();
                    progress = true;
                }
            }

        } while (progress);
    }

    private boolean canDeliver(Classes.LogEntry entry) {
        for (int i = 0; i < numWorkers; i++) {
            if (entry.vectorClock[i] > deliveredClock[i] + 1) {
                return false;
            }
        }
        return true;
    }

    private void deliver(Classes.LogEntry entry) {
        globalLog.add(entry);

        for (int i = 0; i < numWorkers; i++) {
            deliveredClock[i] =
                    Math.max(deliveredClock[i], entry.vectorClock[i]);
        }

        // Write to persistent log
        System.out.println("LOG: " + entry.text
                + " VC=" + Arrays.toString(entry.vectorClock));
    }
}
