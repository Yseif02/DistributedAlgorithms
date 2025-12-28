package VectorClocks;

import java.util.*;




class Worker {

    private final int workerId;
    private final int numWorkers;
    private final int[] vectorClock;

    private final List<Worker> allWorkers;
    private final Logger logger;
    private final Random random = new Random();

    public Worker(int workerId, int numWorkers,
                  List<Worker> allWorkers,
                  Logger logger) {
        this.workerId = workerId;
        this.numWorkers = numWorkers;
        this.allWorkers = allWorkers;
        this.logger = logger;
        this.vectorClock = new int[numWorkers];
    }

    /* =======================
       Core Vector Clock Ops
       ======================= */

    private void incrementClock() {
        vectorClock[workerId]++;
    }

    private void mergeClock(int[] receivedClock) {
        for (int i = 0; i < numWorkers; i++) {
            vectorClock[i] = Math.max(vectorClock[i], receivedClock[i]);
        }
    }

    private int[] copyClock() {
        return Arrays.copyOf(vectorClock, vectorClock.length);
    }

    /* =======================
       Messaging
       ======================= */

    public void sendMessage(Worker target, String payload) {
        incrementClock();

        Classes.Message msg = new Classes.Message(
                workerId,
                payload,
                copyClock()
        );

        // Pretend this goes over the network
        target.receiveMessage(msg);
    }

    public void receiveMessage(Classes.Message msg) {
        mergeClock(msg.vectorClock);
        incrementClock();

        // Log receipt
        logger.receiveMessage(
                new Classes.LogEntry(
                        "Worker " + workerId + " received message from " + msg.senderId,
                        copyClock()
                )
        );
    }

    /* =======================
       Logging
       ======================= */

    private void log(String event) {
        logger.receiveMessage(
                new Classes.LogEntry(event, copyClock())
        );
    }

    /* =======================
       End-User Work
       ======================= */

    public void doWork() {
        // Step 1: local event
        incrementClock();
        log("Worker " + workerId + " does local work");

        // Step 2: do something user-facing
        // (e.g., serve video, send email, etc.)
        // --- do something ---

        // Step 3: randomly communicate with another worker
        Worker target;
        do {
            target = allWorkers.get(random.nextInt(numWorkers));
        } while (target.workerId == this.workerId);

        sendMessage(target, "hello");

        // Step 4: log activity
        log("Worker " + workerId + " sent message to " + target.workerId);
    }
}
